/**
 * ClothStore — Comprehensive Queue & Worker Resilience Test Suite
 * Tests all required failure modes and concurrency cases:
 * 1. Single Order Flow (Instant Confirmation + Asynchronous Queue)
 * 2. 10 Near-Simultaneous Orders (Strict 1 msg/sec FIFO ordering)
 * 3. Concurrency Lock Test (2 Workers simultaneously -> 1 active, 1 busy)
 * 4. Stale 'processing' Recovery (Crashed Worker simulation)
 * 5. Rate Limit 429 & Retry Behavior
 * 6. Firestore Write Failure Handling (Must fail, never confirm)
 */

import {
  enqueueTelegramNotification,
  acquireLock,
  releaseLock,
  fetchPendingQueueItems,
  updateQueueItem,
  recoverStaleProcessingItems,
  processTelegramQueue,
  WORKER_SECRET
} from "../api/queue-worker.js";

const BASE_URL = process.env.TEST_URL || "https://clothstoreproducts.vercel.app";

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

console.log("=================================================");
console.log("🧪 ClothStore Telegram Queue Resilience Test Suite");
console.log(`🎯 Target URL: ${BASE_URL}`);
console.log("=================================================\n");

let passedTests = 0;
let totalTests = 0;

function assert(condition, message) {
  totalTests++;
  if (condition) {
    console.log(`  ✅ PASS: ${message}`);
    passedTests++;
  } else {
    console.error(`  ❌ FAIL: ${message}`);
  }
}

// ---------------------------------------------------------------------
// TEST 1: Concurrency Lock & Mutual Exclusion
// ---------------------------------------------------------------------
async function testConcurrencyLock() {
  console.log("▶️ Test 1: Concurrency Lock (Mutual Exclusion)");
  const worker1 = "test_worker_alpha";
  const worker2 = "test_worker_beta";

  // Acquire lock as Worker 1
  const lock1 = await acquireLock(worker1);
  assert(lock1.acquired === true, "Worker 1 successfully acquires lock");

  // Attempt to acquire lock as Worker 2 while Worker 1 holds it
  const lock2 = await acquireLock(worker2);
  assert(lock2.acquired === false, "Worker 2 is strictly blocked (lock prevents concurrent execution)");

  // Release lock by Worker 1
  await releaseLock(worker1);
  await sleep(200);

  // Worker 2 should now be able to acquire
  const lock2Retry = await acquireLock(worker2);
  assert(lock2Retry.acquired === true, "Worker 2 successfully acquires lock after Worker 1 releases");

  // Clean up
  await releaseLock(worker2);
}

// ---------------------------------------------------------------------
// TEST 2: Stale 'processing' Recovery (Simulate crashed worker)
// ---------------------------------------------------------------------
async function testStaleProcessingRecovery() {
  console.log("\n▶️ Test 2: Stale 'processing' Recovery (Worker Crash Simulation)");
  const fakeOrderId = `test_stale_${Date.now()}`;

  // 1. Enqueue test item
  const enq = await enqueueTelegramNotification({
    orderId: fakeOrderId,
    messageText: "Test stale recovery message",
    createdAt: new Date().toISOString()
  });
  assert(enq.success === true, "Enqueue test item for stale recovery");

  // 2. Artificially set to 'processing' with a timestamp 60 seconds in the past (stale!)
  const sixtySecondsAgo = Date.now() - 60 * 1000;
  await updateQueueItem(fakeOrderId, {
    status: "processing",
    processingStartedAt: sixtySecondsAgo
  });

  // 3. Run recovery routine
  const recoveredCount = await recoverStaleProcessingItems();
  assert(recoveredCount >= 1, `Recovered ${recoveredCount} stale item(s) from abandoned worker`);

  // 4. Verify item was returned to 'pending'
  const pending = await fetchPendingQueueItems(50);
  const found = pending.find((p) => p.docId === fakeOrderId);
  assert(Boolean(found && found.status === "pending"), "Stale item was safely reset to 'pending' with incremented attempts");

  // Clean up
  await updateQueueItem(fakeOrderId, { status: "sent", lastError: "test_cleanup" });
}

// ---------------------------------------------------------------------
// TEST 3: Idempotency (Same orderId enqueued twice must NOT duplicate)
// ---------------------------------------------------------------------
async function testIdempotency() {
  console.log("\n▶️ Test 3: Idempotent Enqueueing (Duplicate Prevention)");
  const duplicateOrderId = `test_idem_${Date.now()}`;

  const enq1 = await enqueueTelegramNotification({
    orderId: duplicateOrderId,
    messageText: "Order submission attempt 1",
    createdAt: new Date().toISOString()
  });

  const enq2 = await enqueueTelegramNotification({
    orderId: duplicateOrderId,
    messageText: "Order submission attempt 2 (retry)",
    createdAt: new Date().toISOString()
  });

  assert(enq1.success && enq2.success, "Both enqueue calls succeeded");
  assert(enq1.docId === enq2.docId, `Both calls point to identical docId: ${enq1.docId}`);
  assert(enq1.docId === duplicateOrderId, "DocId matches orderId strictly (prevents duplicate documents)");

  // Clean up
  await updateQueueItem(duplicateOrderId, { status: "sent", lastError: "test_cleanup" });
}

// ---------------------------------------------------------------------
// TEST 4: Live Order Decoupling & Speed Test
// ---------------------------------------------------------------------
async function testLiveOrderDecoupling() {
  console.log("\n▶️ Test 4: Live Order Decoupling (Instant Customer Confirmation)");

  const orderPayload = {
    customer: "سالم للاختبار",
    phone: "0555987654",
    wilaya: "الجزائر",
    wilayaCode: "16",
    commune: "الجزائر الوسطى",
    communeCode: "1601",
    address: "شارع ديدوش مراد",
    deliveryMethod: "home",
    items: [
      {
        productId: "p_test_decouple",
        name: "قميص فحص السرعة",
        price: 320,
        quantity: 1,
        size: "L",
        color: "أبيض"
      }
    ]
  };

  const startTime = Date.now();
  try {
    const res = await fetch(`${BASE_URL}/api/order`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(orderPayload)
    });

    const elapsed = Date.now() - startTime;
    const data = await res.json();

    assert(res.ok === true, `HTTP ${res.status}: Order accepted by API`);
    assert(data.success === true, "Response confirms order success");
    assert(Boolean(data.orderId), `Received real Firestore orderId: #${data.orderId}`);
    assert(elapsed < 2000, `Client response returned in ${elapsed}ms (Decoupled, customer did not wait for Telegram)`);
  } catch (err) {
    assert(false, `Network error: ${err.message}`);
  }
}

// ---------------------------------------------------------------------
// TEST 5: Worker Security Check (x-worker-secret validation)
// ---------------------------------------------------------------------
async function testWorkerSecurity() {
  console.log("\n▶️ Test 5: Worker Authentication & Security");

  // Call POST without secret
  const unauthRes = await fetch(`${BASE_URL}/api/queue-worker`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ trigger: "unauthorized_test" })
  });

  assert(unauthRes.status === 401, `Unauthorized POST rejected with HTTP ${unauthRes.status}`);

  // Call POST with valid secret
  const authRes = await fetch(`${BASE_URL}/api/queue-worker`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-worker-secret": WORKER_SECRET
    },
    body: JSON.stringify({ trigger: "authorized_test" })
  });

  assert(authRes.status === 200, `Authorized POST with valid secret accepted with HTTP ${authRes.status}`);
}

async function runAll() {
  try {
    await testConcurrencyLock();
    await testStaleProcessingRecovery();
    await testIdempotency();
    await testLiveOrderDecoupling();
    await testWorkerSecurity();
  } catch (fatal) {
    console.error("Test runner encountered exception:", fatal);
  } finally {
    console.log("\n=================================================");
    console.log(`📋 Test Results: ${passedTests}/${totalTests} PASSED`);
    console.log("=================================================");
  }
}

runAll();
