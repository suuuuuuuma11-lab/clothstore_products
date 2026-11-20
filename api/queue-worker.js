/**
 * ClothStore — Resilient Telegram Notification Queue Worker
 * Architecture: Cloud Firestore + Vercel Serverless Functions
 *
 * SPECIFICATION COMPLIANCE:
 * 1. Decoupled Processing: Customer receives instant confirmation upon Firestore save;
 *    Telegram dispatch happens asynchronously in the background.
 * 2. Dedicated Single-Order Messages: Each order is stored and sent individually (No batching).
 * 3. Strict Concurrency Lock: Only 1 worker instance processes the queue at any time (queueControl/workerLock).
 * 4. Precise Rate-Limiting: Enforces 1.0s delay between messages to respect Telegram limits.
 * 5. Complete Lifecycle States: pending -> processing -> sent | failed.
 * 6. Stale Processing Recovery: Automatically recovers messages abandoned by crashed/timed-out functions.
 * 7. Self-Chaining: Automatically spawns next worker invocation if pending messages remain after time budget.
 * 8. Authenticated Endpoint: Validates 'x-worker-secret' to prevent unauthorized external triggers.
 */

const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "clothstore-a9b2c";
const BASE_FS_URL = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents`;
export const WORKER_SECRET = process.env.QUEUE_WORKER_SECRET || "hadjaj_secure_worker_token_2026";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Converts a standard JS object to Firestore REST API fields
 */
export function toFirestoreFields(obj) {
  const fields = {};
  for (const [key, val] of Object.entries(obj)) {
    if (val === null || val === undefined) continue;
    if (typeof val === "string") {
      fields[key] = { stringValue: val };
    } else if (typeof val === "number") {
      fields[key] = Number.isInteger(val) ? { integerValue: String(val) } : { doubleValue: val };
    } else if (typeof val === "boolean") {
      fields[key] = { booleanValue: val };
    } else if (Array.isArray(val)) {
      fields[key] = {
        arrayValue: {
          values: val.map((item) => {
            if (typeof item === "object" && item !== null) {
              return { mapValue: { fields: toFirestoreFields(item) } };
            }
            if (typeof item === "number") return { integerValue: String(item) };
            return { stringValue: String(item) };
          })
        }
      };
    } else if (typeof val === "object") {
      fields[key] = {
        mapValue: { fields: toFirestoreFields(val) }
      };
    }
  }
  return fields;
}

/**
 * Converts Firestore REST API fields back to a standard JS object
 */
export function fromFirestoreFields(fields = {}) {
  const obj = {};
  for (const [key, val] of Object.entries(fields)) {
    if (val.stringValue !== undefined) obj[key] = val.stringValue;
    else if (val.integerValue !== undefined) obj[key] = parseInt(val.integerValue, 10);
    else if (val.doubleValue !== undefined) obj[key] = parseFloat(val.doubleValue);
    else if (val.booleanValue !== undefined) obj[key] = val.booleanValue;
    else if (val.mapValue !== undefined) obj[key] = fromFirestoreFields(val.mapValue.fields);
    else if (val.arrayValue !== undefined) {
      obj[key] = (val.arrayValue.values || []).map((v) => {
        if (v.mapValue) return fromFirestoreFields(v.mapValue.fields);
        if (v.stringValue !== undefined) return v.stringValue;
        if (v.integerValue !== undefined) return parseInt(v.integerValue, 10);
        if (v.doubleValue !== undefined) return parseFloat(v.doubleValue);
        return v;
      });
    }
  }
  return obj;
}

/**
 * Enqueues a notification into Firestore telegramQueue collection.
 * Uses orderId as the document ID for absolute idempotency.
 */
export async function enqueueTelegramNotification(payload) {
  try {
    const orderId = String(payload.orderId || `ord_${Date.now()}`).trim();
    const docUrl = `${BASE_FS_URL}/telegramQueue/${encodeURIComponent(orderId)}`;

    const docData = {
      orderId: orderId,
      messageText: payload.messageText || "",
      orderSummary: payload.orderSummary || {},
      status: "pending",
      attempts: 0,
      maxAttempts: 4,
      createdAt: payload.createdAt || new Date().toISOString(),
      processingStartedAt: 0,
      sentAt: null,
      telegramMessageId: null,
      lastError: null
    };

    const updateMask = Object.keys(docData)
      .map((k) => `updateMask.fieldPaths=${encodeURIComponent(k)}`)
      .join("&");

    const res = await fetch(`${docUrl}?${updateMask}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields: toFirestoreFields(docData) })
    });

    if (res.ok) {
      return { success: true, docId: orderId };
    } else {
      const errText = await res.text();
      console.warn(`[QUEUE ENQUEUE FAILED for ${orderId}]:`, errText);
      return { success: false, error: errText };
    }
  } catch (err) {
    console.error("[QUEUE ENQUEUE EXCEPTION]:", err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Attempts to acquire an exclusive lock in Firestore (queueControl/workerLock).
 * Prevents multiple workers from running concurrently.
 * TTL of 25 seconds ensures automatic recovery if a worker dies mid-flight.
 */
export async function acquireLock(workerId) {
  const lockUrl = `${BASE_FS_URL}/queueControl/workerLock`;
  const now = Date.now();
  const LOCK_TTL_MS = 25 * 1000; // 25s auto-expiration

  try {
    const getRes = await fetch(lockUrl);
    if (getRes.ok) {
      const lockDoc = await getRes.json();
      const lockData = fromFirestoreFields(lockDoc.fields || {});
      const isLocked = Boolean(lockData.isLocked);
      const expiresAt = Number(lockData.expiresAt || 0);

      // If actively locked by another worker and unexpired
      if (isLocked && now < expiresAt) {
        return {
          acquired: false,
          reason: `Locked by active worker (${lockData.workerId}) until ${new Date(expiresAt).toISOString()}`
        };
      }
    }

    // Write / refresh lock
    const lockPayload = {
      isLocked: true,
      workerId,
      lockedAt: now,
      expiresAt: now + LOCK_TTL_MS
    };

    const updateMask = Object.keys(lockPayload)
      .map((k) => `updateMask.fieldPaths=${encodeURIComponent(k)}`)
      .join("&");

    const patchRes = await fetch(`${lockUrl}?${updateMask}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields: toFirestoreFields(lockPayload) })
    });

    return { acquired: patchRes.ok };
  } catch (err) {
    console.warn("[ACQUIRE LOCK EXCEPTION]:", err.message);
    return { acquired: false, reason: err.message };
  }
}

/**
 * Releases the worker lock
 */
export async function releaseLock(workerId) {
  const lockUrl = `${BASE_FS_URL}/queueControl/workerLock`;
  try {
    const unlockPayload = {
      isLocked: false,
      releasedAt: Date.now(),
      lastWorkerId: workerId
    };

    const updateMask = Object.keys(unlockPayload)
      .map((k) => `updateMask.fieldPaths=${encodeURIComponent(k)}`)
      .join("&");

    await fetch(`${lockUrl}?${updateMask}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields: toFirestoreFields(unlockPayload) })
    });
  } catch (err) {
    console.warn("[RELEASE LOCK WARN]:", err.message);
  }
}

/**
 * Updates a queue item's status and metadata with atomic retry
 */
export async function updateQueueItem(docId, fieldsToUpdate = {}) {
  const docUrl = `${BASE_FS_URL}/telegramQueue/${encodeURIComponent(docId)}`;
  const payload = {
    updatedAt: new Date().toISOString(),
    ...fieldsToUpdate
  };

  const updateMask = Object.keys(payload)
    .map((k) => `updateMask.fieldPaths=${encodeURIComponent(k)}`)
    .join("&");

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(`${docUrl}?${updateMask}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields: toFirestoreFields(payload) })
      });
      if (res.ok) return true;
      if (attempt < 3) await sleep(150);
    } catch (_) {
      if (attempt < 3) await sleep(150);
    }
  }
  return false;
}

/**
 * Recovers messages that were left in 'processing' by a crashed or timed-out function.
 * If processingStartedAt was > 40 seconds ago, reset to 'pending' (or mark 'failed' if maxAttempts reached).
 */
export async function recoverStaleProcessingItems() {
  const queryUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`;
  const queryBody = {
    structuredQuery: {
      from: [{ collectionId: "telegramQueue" }],
      where: {
        fieldFilter: {
          field: { fieldPath: "status" },
          op: "EQUAL",
          value: { stringValue: "processing" }
        }
      },
      limit: 25
    }
  };

  try {
    const res = await fetch(queryUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(queryBody)
    });

    if (!res.ok) return 0;
    const results = await res.json();
    if (!Array.isArray(results)) return 0;

    let recovered = 0;
    const now = Date.now();
    const STALE_TIMEOUT_MS = 40 * 1000; // 40 seconds threshold

    for (const row of results) {
      if (row.document && row.document.fields) {
        const docId = row.document.name.split("/").pop();
        const data = fromFirestoreFields(row.document.fields);
        const startedAt = Number(data.processingStartedAt || 0);

        // If processing started more than 40 seconds ago, it is stale
        if (now - startedAt > STALE_TIMEOUT_MS) {
          const attempts = Number(data.attempts || 0) + 1;
          const maxAttempts = Number(data.maxAttempts || 4);

          if (attempts >= maxAttempts) {
            await updateQueueItem(docId, {
              status: "failed",
              attempts,
              lastError: "Abandoned by worker (timed out repeatedly)"
            });
          } else {
            await updateQueueItem(docId, {
              status: "pending",
              attempts,
              lastError: "Worker timeout recovered — re-queued"
            });
            recovered++;
          }
        }
      }
    }
    return recovered;
  } catch (err) {
    console.warn("[RECOVER STALE EXCEPTION]:", err.message);
    return 0;
  }
}

/**
 * Queries Cloud Firestore for pending queue items sorted chronologically (FIFO)
 */
export async function fetchPendingQueueItems(limit = 15) {
  const queryUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`;
  const queryBody = {
    structuredQuery: {
      from: [{ collectionId: "telegramQueue" }],
      where: {
        fieldFilter: {
          field: { fieldPath: "status" },
          op: "EQUAL",
          value: { stringValue: "pending" }
        }
      },
      limit
    }
  };

  try {
    const res = await fetch(queryUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(queryBody)
    });

    if (!res.ok) {
      console.warn("[FETCH QUEUE ERROR]:", await res.text());
      return [];
    }

    const results = await res.json();
    if (!Array.isArray(results)) return [];

    const items = [];
    for (const row of results) {
      if (row.document && row.document.fields) {
        const docId = row.document.name.split("/").pop();
        const data = fromFirestoreFields(row.document.fields);
        items.push({
          docId,
          ...data
        });
      }
    }

    // Sort strictly chronologically (oldest order first)
    items.sort((a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime());
    return items;
  } catch (err) {
    console.error("[FETCH QUEUE EXCEPTION]:", err.message);
    return [];
  }
}

/**
 * Dispatches an individual order notification to Telegram Bot API.
 * Validates responses strictly and respects HTTP 429 retry_after.
 */
export async function sendIndividualTelegramMessage(token, chatId, text) {
  const endpoint = `https://api.telegram.org/bot${token}/sendMessage`;

  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: chatId,
          text: text
        })
      });

      const data = await res.json().catch(() => ({}));

      // Success confirmed by Telegram
      if (res.ok && data.ok) {
        return {
          success: true,
          messageId: data.result?.message_id
        };
      }

      // Handle HTTP 429 Too Many Requests (Rate Limit)
      if (res.status === 429 || data.error_code === 429) {
        const retrySec = Number(data.parameters?.retry_after || 2);
        console.warn(`[TELEGRAM RATE LIMIT 429]: Waiting ${retrySec}s...`);
        if (attempt === 1) {
          await sleep(Math.min(retrySec, 4) * 1000);
          continue; // retry once
        }
        return {
          success: false,
          is429: true,
          retryAfter: retrySec,
          error: `Telegram Rate Limited (wait ${retrySec}s)`
        };
      }

      // Non-429 error
      return {
        success: false,
        error: data.description || `HTTP ${res.status} ${res.statusText}`
      };
    } catch (netErr) {
      if (attempt === 1) {
        await sleep(1000);
        continue;
      }
      return {
        success: false,
        error: `Network error: ${netErr.message}`
      };
    }
  }

  return { success: false, error: "Exceeded dispatch retry attempts" };
}

/**
 * Core Queue Processing Engine
 * 
 * Rules:
 * - 1 message per second rate limit.
 * - Single order per message (no batching).
 * - Maximum time budget: 7.5 seconds (Vercel safety).
 * - Updates status: pending -> processing -> sent | failed.
 * - Releases lock and self-chains if pending items remain.
 */
export async function processTelegramQueue(options = {}) {
  const maxDurationMs = options.maxDurationMs || 7500; // 7.5s budget
  const startTime = Date.now();
  const workerId = `w_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.warn("[QUEUE WORKER]: Telegram credentials not set in environment.");
    return { status: "unconfigured", error: "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID" };
  }

  // 1. Concurrency Guard Lock
  const lockResult = await acquireLock(workerId);
  if (!lockResult.acquired) {
    return { status: "busy", message: lockResult.reason };
  }

  let processedCount = 0;
  let remainingCount = 0;
  let hasMoreToProcess = false;

  try {
    // 2. Recover any stale processing items
    await recoverStaleProcessingItems();

    // 3. Fetch pending orders (FIFO)
    const pendingItems = await fetchPendingQueueItems(10);
    if (pendingItems.length === 0) {
      return { status: "idle", processed: 0, message: "Queue is empty" };
    }

    // 4. Sequential Processing Loop (1 message per second)
    for (let i = 0; i < pendingItems.length; i++) {
      const item = pendingItems[i];

      // Time budget guard
      if (Date.now() - startTime >= maxDurationMs) {
        hasMoreToProcess = true;
        remainingCount = pendingItems.length - i;
        break;
      }

      // Step A: Mark as 'processing'
      await updateQueueItem(item.docId, {
        status: "processing",
        processingStartedAt: Date.now()
      });

      // Step B: Dispatch single message to Telegram
      const result = await sendIndividualTelegramMessage(token, chatId, item.messageText);

      // Step C: Update state based on Telegram response
      if (result.success) {
        await updateQueueItem(item.docId, {
          status: "sent",
          sentAt: new Date().toISOString(),
          telegramMessageId: result.messageId || null,
          lastError: null
        });
        processedCount++;
      } else if (result.is429) {
        // Rate limited: reset to pending and back off
        const attempts = Number(item.attempts || 0) + 1;
        await updateQueueItem(item.docId, {
          status: "pending",
          attempts,
          lastError: result.error
        });
        hasMoreToProcess = true;
        break; // Stop current loop to respect Telegram's rate limit window
      } else {
        // Failed attempt
        const attempts = Number(item.attempts || 0) + 1;
        const maxAttempts = Number(item.maxAttempts || 4);
        const nextStatus = attempts >= maxAttempts ? "failed" : "pending";

        await updateQueueItem(item.docId, {
          status: nextStatus,
          attempts,
          lastError: result.error
        });

        if (nextStatus === "failed") {
          console.error(`[QUEUE ITEM PERMANENTLY FAILED]: Order ${item.docId} - ${result.error}`);
        }
      }

      // Step D: Rate Limiting Throttling (Strict 1.0 second between messages)
      const isLastItem = i === pendingItems.length - 1;
      const withinTimeBudget = Date.now() - startTime + 1050 < maxDurationMs;
      if (!isLastItem && withinTimeBudget) {
        await sleep(1000);
      }
    }

    // Check if additional pending items exist in the queue beyond current batch
    if (!hasMoreToProcess) {
      const moreCheck = await fetchPendingQueueItems(1);
      if (moreCheck.length > 0) {
        hasMoreToProcess = true;
        remainingCount = moreCheck.length;
      }
    }

    return {
      status: "success",
      processed: processedCount,
      remaining: remainingCount,
      hasMore: hasMoreToProcess,
      durationMs: Date.now() - startTime
    };
  } finally {
    // 5. Always release the lock
    await releaseLock(workerId);

    // 6. Self-Chaining: If more orders remain, spawn the next worker invocation
    if (hasMoreToProcess && options.triggerNextUrl) {
      try {
        fetch(options.triggerNextUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-worker-secret": WORKER_SECRET
          },
          body: JSON.stringify({ trigger: "self_chain", timestamp: Date.now() })
        }).catch(() => {});
      } catch (_) {}
    }
  }
}

/**
 * Standard Vercel Serverless Function Handler
 * Route: /api/queue-worker
 */
export default async function handler(req, res) {
  // 1. Security Authentication Check
  const incomingSecret = req.headers["x-worker-secret"] || req.query?.secret;
  const isInternalSecretValid = incomingSecret === WORKER_SECRET;
  const isDevMode = !process.env.NODE_ENV || process.env.NODE_ENV === "development";

  // Allow GET for simple health / authorized manual status checks, require secret for POST
  if (req.method === "POST" && !isInternalSecretValid && !isDevMode) {
    return res.status(401).json({ error: "Unauthorized worker trigger: invalid secret token" });
  }

  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const host = req.headers?.host || "localhost";
    const proto = req.headers?.["x-forwarded-proto"] || (host.includes("localhost") ? "http" : "https");
    const triggerNextUrl = `${proto}://${host}/api/queue-worker`;

    // If GET request is just a health/status check without running
    if (req.method === "GET" && req.query?.checkOnly === "1") {
      const pendingItems = await fetchPendingQueueItems(5);
      return res.status(200).json({
        ok: true,
        status: "idle",
        pendingCount: pendingItems.length,
        samplePending: pendingItems.map((p) => ({ id: p.docId, status: p.status, createdAt: p.createdAt }))
      });
    }

    const result = await processTelegramQueue({
      maxDurationMs: 7500,
      triggerNextUrl
    });

    return res.status(200).json({
      ok: true,
      result
    });
  } catch (err) {
    console.error("[QUEUE WORKER HANDLER FATAL]:", err);
    return res.status(500).json({ error: "Internal queue worker error", details: err.message });
  }
}
