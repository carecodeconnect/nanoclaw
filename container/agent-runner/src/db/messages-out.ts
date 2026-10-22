/**
 * Outbound message operations (container side).
 *
 * Writes to outbound.db (container-owned).
 * The host polls this DB (read-only) for undelivered messages.
 */
import { getInboundDb, getOutboundDb } from './connection.js';

export interface MessageOutRow {
  id: string;
  seq: number | null;
  in_reply_to: string | null;
  timestamp: string;
  deliver_after: string | null;
  recurrence: string | null;
  kind: string;
  platform_id: string | null;
  channel_type: string | null;
  thread_id: string | null;
  content: string;
}

export interface WriteMessageOut {
  id: string;
  in_reply_to?: string | null;
  deliver_after?: string | null;
  recurrence?: string | null;
  kind: string;
  platform_id?: string | null;
  channel_type?: string | null;
  thread_id?: string | null;
  content: string;
}

/**
 * Write a new outbound message, auto-assigning an odd seq number.
 * Container uses odd seq (1, 3, 5...), host uses even (2, 4, 6...).
 *
 * The disjoint namespace is load-bearing, not just collision avoidance:
 * seq is the agent-facing message ID returned by send_message and accepted
 * by edit_message / add_reaction, and getMessageIdBySeq() below looks up
 * by seq across BOTH tables. If inbound and outbound could share a seq,
 * the agent's "edit message #5" could resolve to the wrong row.
 */
/**
 * How long an identical outbound message is treated as a duplicate.
 *
 * Sized from the observed failure: an agent delivered the same 1469-char reply
 * twice, 5.1s apart (and a 1242-char one 5.2s apart). 30s covers that with
 * margin while staying far below any plausible interval for a human
 * legitimately asking for the same text again.
 */
const DUPLICATE_WINDOW_SECONDS = 30;

/**
 * Suppress an outbound message identical to one queued moments ago.
 *
 * WHY THIS EXISTS. NanoClaw has two independent ways to deliver a message and
 * nothing reconciled them:
 *
 *   1. the `send_message` MCP tool        (mcp-tools/core.ts)
 *   2. the `<message to="...">` envelope  (poll-loop.ts sendToDestination)
 *
 * Both funnel through writeMessageOut, but neither knew about the other. On
 * 2026-07-30 an agent answered a request by calling send_message AND emitting
 * the same text as an envelope, so the user received the identical message
 * twice, ~5s apart, with different ids. Telling the agent not to do that is
 * worth doing, but a prompt is not a guarantee — this is the guarantee.
 *
 * Deliberately narrow, because suppressing a message the user should have
 * received is worse than a rare duplicate:
 *   - only `chat` kind (task_log / file / system kinds are not user-facing
 *     prose and may legitimately repeat);
 *   - only immediate sends — anything with deliver_after or recurrence is a
 *     SCHEDULED message, and a recurring digest repeating its text is correct;
 *   - identical content AND identical destination (channel_type, platform_id,
 *     thread_id), compared with IS so NULLs match rather than never matching.
 *
 * Returns the existing row's seq when it suppresses, so the caller still gets a
 * usable message id for edit_message / add_reaction — those then target the
 * message that was actually delivered.
 */
function findRecentDuplicateSeq(msg: WriteMessageOut): number | null {
  if (msg.kind !== 'chat') return null;
  if (msg.deliver_after || msg.recurrence) return null;
  if (!msg.content || !msg.content.trim()) return null;

  const row = getOutboundDb()
    .prepare(
      `SELECT seq FROM messages_out
        WHERE kind = 'chat'
          AND content = $content
          AND channel_type IS $channel_type
          AND platform_id  IS $platform_id
          AND thread_id    IS $thread_id
          AND datetime(timestamp) >= datetime('now', $window)
        ORDER BY timestamp DESC
        LIMIT 1`,
    )
    .get({
      $content: msg.content,
      $channel_type: msg.channel_type ?? null,
      $platform_id: msg.platform_id ?? null,
      $thread_id: msg.thread_id ?? null,
      $window: `-${DUPLICATE_WINDOW_SECONDS} seconds`,
    }) as { seq: number } | undefined;

  return row?.seq ?? null;
}

export function writeMessageOut(msg: WriteMessageOut): number {
  const outbound = getOutboundDb();
  const inbound = getInboundDb();

  // Never silently: a suppression must be visible in the container log, or a
  // genuinely-lost message would look identical to a working system.
  const duplicateSeq = findRecentDuplicateSeq(msg);
  if (duplicateSeq !== null) {
    console.warn(
      `[messages-out] suppressed duplicate ${msg.kind} to ` +
        `${msg.channel_type ?? '?'}:${msg.platform_id ?? '?'} ` +
        `(${msg.content.length} chars, identical within ` +
        `${DUPLICATE_WINDOW_SECONDS}s) — returning existing seq ${duplicateSeq}`,
    );
    return duplicateSeq;
  }

  // Read max seq from both DBs to maintain global ordering.
  // Safe: each side only reads the other DB, never writes to it.
  const maxOut = (outbound.prepare('SELECT COALESCE(MAX(seq), 0) AS m FROM messages_out').get() as { m: number }).m;
  const maxIn = (inbound.prepare('SELECT COALESCE(MAX(seq), 0) AS m FROM messages_in').get() as { m: number }).m;
  const max = Math.max(maxOut, maxIn);
  const nextSeq = max % 2 === 0 ? max + 1 : max + 2; // next odd

  // bun:sqlite requires named parameters to be passed with the prefix character
  // in the JS object keys (better-sqlite3 auto-stripped it, bun:sqlite does not).
  outbound
    .prepare(
      `INSERT INTO messages_out (id, seq, in_reply_to, timestamp, deliver_after, recurrence, kind, platform_id, channel_type, thread_id, content)
     VALUES ($id, $seq, $in_reply_to, $timestamp, $deliver_after, $recurrence, $kind, $platform_id, $channel_type, $thread_id, $content)`,
    )
    .run({
      $id: msg.id,
      $seq: nextSeq,
      $timestamp: new Date().toISOString(),
      $in_reply_to: msg.in_reply_to ?? null,
      $deliver_after: msg.deliver_after ?? null,
      $recurrence: msg.recurrence ?? null,
      $kind: msg.kind,
      $platform_id: msg.platform_id ?? null,
      $channel_type: msg.channel_type ?? null,
      $thread_id: msg.thread_id ?? null,
      $content: msg.content,
    });

  return nextSeq;
}

/**
 * Look up a message's platform ID by seq number.
 * Searches both inbound and outbound DBs since seq spans both.
 *
 * For inbound messages, the Chat SDK message ID is already the platform message ID
 * (e.g., "6037840640:42" for Telegram).
 *
 * For outbound messages, the internal ID (msg-xxx) won't work for edits/reactions.
 * Instead, look up the platform_message_id from the delivered table (host writes this
 * after successful delivery).
 */
export function getMessageIdBySeq(seq: number): string | null {
  const inbound = getInboundDb();

  // Inbound messages: ID is already the platform message ID
  const inRow = inbound.prepare('SELECT id FROM messages_in WHERE seq = ?').get(seq) as
    | { id: string }
    | undefined;
  if (inRow) return inRow.id;

  // Outbound messages: look up platform message ID from delivered table
  const outRow = getOutboundDb().prepare('SELECT id FROM messages_out WHERE seq = ?').get(seq) as
    | { id: string }
    | undefined;
  if (!outRow) return null;

  // Check if host has stored the platform message ID after delivery
  const deliveredRow = inbound
    .prepare('SELECT platform_message_id FROM delivered WHERE message_out_id = ?')
    .get(outRow.id) as { platform_message_id: string | null } | undefined;
  if (deliveredRow?.platform_message_id) return deliveredRow.platform_message_id;

  // Fallback to internal ID (edits/reactions on undelivered messages won't work)
  return outRow.id;
}

/**
 * Look up the routing fields for a message by seq (for edit/reaction targeting).
 * Returns the channel_type, platform_id, thread_id of the referenced message.
 */
export function getRoutingBySeq(
  seq: number,
): { channel_type: string | null; platform_id: string | null; thread_id: string | null } | null {
  const inbound = getInboundDb();
  const inRow = inbound
    .prepare('SELECT channel_type, platform_id, thread_id FROM messages_in WHERE seq = ?')
    .get(seq) as { channel_type: string | null; platform_id: string | null; thread_id: string | null } | undefined;
  if (inRow) return inRow;

  const outRow = getOutboundDb()
    .prepare('SELECT channel_type, platform_id, thread_id FROM messages_out WHERE seq = ?')
    .get(seq) as { channel_type: string | null; platform_id: string | null; thread_id: string | null } | undefined;
  return outRow ?? null;
}

/** Get undelivered messages (for host polling — reads from outbound.db). */
export function getUndeliveredMessages(): MessageOutRow[] {
  return getOutboundDb()
    .prepare(
      `SELECT * FROM messages_out
       WHERE (deliver_after IS NULL OR datetime(deliver_after) <= datetime('now'))
       ORDER BY timestamp ASC`,
    )
    .all() as MessageOutRow[];
}
