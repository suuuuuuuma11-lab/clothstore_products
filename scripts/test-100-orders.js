/**
 * ClothStore — 100 Orders Stress Test & Simulation Script
 * Simulates high-burst traffic to test the Telegram Queue, Firestore, and Store Performance.
 * 
 * Usage:
 *   node scripts/test-100-orders.js <SITE_URL> [COUNT]
 * 
 * Examples:
 *   node scripts/test-100-orders.js https://clothstore-products.vercel.app 100
 *   node scripts/test-100-orders.js http://localhost:3000 10
 */

const TARGET_URL = process.argv[2] || "https://clothstoreproducts.vercel.app";
const ORDER_COUNT = parseInt(process.argv[3] || "100", 10);

const FIRST_NAMES = ["أحمد", "محمد", "كريم", "يوسف", "أمين", "وليد", "هشام", "ياسين", "مراد", "بلال", "سفيان", "عبد الرحيم", "إسماعيل", "فؤاد", "رضا"];
const LAST_NAMES = ["بن علي", "بلحاج", "بوزيد", "منصوري", "مزيان", "قاسمي", "سعيدي", "حمدي", "داودي", "عمراني", "طاهري", "رحماني", "سليماني"];

const SAMPLE_WILAYAS = [
  { code: "16", name: "الجزائر", commune: "الجزائر الوسطى", communeCode: "1601" },
  { code: "31", name: "وهران", commune: "وهران", communeCode: "3101" },
  { code: "25", name: "قسنطينة", commune: "قسنطينة", communeCode: "2501" },
  { code: "19", name: "سطيف", commune: "سطيف", communeCode: "1901" },
  { code: "06", name: "بجاية", commune: "بجاية", communeCode: "0601" },
  { code: "09", name: "البليدة", commune: "البليدة", communeCode: "0901" },
  { code: "13", name: "تلمسان", commune: "تلمسان", communeCode: "1301" },
  { code: "23", name: "عنابة", commune: "عنابة", communeCode: "2301" },
  { code: "15", name: "تيزي وزو", commune: "تيزي وزو", communeCode: "1501" },
  { code: "05", name: "باتنة", commune: "باتنة", communeCode: "0501" }
];

const PRODUCTS = [
  { name: "قميص كلاسيك فاخر", price: 299 },
  { name: "قميص كلاسيك أسود", price: 320 },
  { name: "قميص بأزرار أنيقة", price: 320 },
  { name: "قميص كاجوال شبابي", price: 280 }
];

const SIZES = ["M", "L", "XL", "XXL"];
const COLORS = [
  { name: "أبيض عاجي", hex: "#f5f2ea" },
  { name: "أسود داكن", hex: "#18181b" },
  { name: "أزرق كحلي", hex: "#1e293b" },
  { name: "رمادي فاخر", hex: "#71717a" }
];

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateOrder(index) {
  const fName = pickRandom(FIRST_NAMES);
  const lName = pickRandom(LAST_NAMES);
  const wilaya = pickRandom(SAMPLE_WILAYAS);
  const method = Math.random() > 0.4 ? "home" : "stopDesk";

  // Generate valid 10-digit Algerian phone (05, 06, 07 + 8 digits)
  const prefix = pickRandom(["05", "06", "07"]);
  const randomEight = String(Math.floor(10000000 + Math.random() * 90000000));
  const phone = `${prefix}${randomEight}`;

  const prod = pickRandom(PRODUCTS);
  const size = pickRandom(SIZES);
  const color = pickRandom(COLORS);
  const qty = Math.random() > 0.8 ? 2 : 1;

  return {
    customer: `${fName} ${lName}`,
    phone: phone,
    wilaya: wilaya.name,
    wilayaCode: wilaya.code,
    commune: wilaya.commune,
    communeCode: wilaya.communeCode,
    address: `حي السلام، عمارة ${index + 1}`,
    deliveryMethod: method,
    items: [
      {
        productId: `p_sim_${(index % 4) + 1}`,
        name: prod.name,
        price: prod.price,
        quantity: qty,
        size: size,
        color: color.name,
        colorHex: color.hex
      }
    ]
  };
}

async function runSimulation() {
  const cleanEndpoint = TARGET_URL.replace(/\/+$/, "") + "/api/order";

  console.log("=================================================");
  console.log("🚀 بدء محاكاة ضغط الطلبات (Order Load Simulation)");
  console.log(`🎯 الرابط المستهدف: ${cleanEndpoint}`);
  console.log(`📦 عدد الطلبات المطلوب إرسالها: ${ORDER_COUNT} طلب`);
  console.log("=================================================\n");

  const startTime = Date.now();
  let successCount = 0;
  let failCount = 0;

  // Dispatch orders concurrently in controlled bursts
  const BATCH_SIZE = 10;
  for (let i = 0; i < ORDER_COUNT; i += BATCH_SIZE) {
    const chunkPromises = [];
    const chunkLimit = Math.min(i + BATCH_SIZE, ORDER_COUNT);

    for (let j = i; j < chunkLimit; j++) {
      const orderPayload = generateOrder(j);
      // Simulate distinct customer IPs from Algerian telecom pool to bypass single-IP limits
      const simulatedIp = `105.101.${Math.floor(j / 250)}.${(j % 250) + 1}`;

      const p = fetch(cleanEndpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Forwarded-For": simulatedIp
        },
        body: JSON.stringify(orderPayload)
      })
        .then(async (res) => {
          const data = await res.json().catch(() => ({}));
          if (res.ok && data.success) {
            successCount++;
            const shortId = (data.orderId || "OK").slice(-6).toUpperCase();
            console.log(`[طلب ${j + 1}/${ORDER_COUNT}] ✅ تم الحفظ بنجاح: #${shortId} | الزبون: ${orderPayload.customer} (${orderPayload.wilaya})`);
          } else {
            failCount++;
            console.warn(`[طلب ${j + 1}/${ORDER_COUNT}] ❌ فشل: ${data.error || res.statusText}`);
          }
        })
        .catch((err) => {
          failCount++;
          console.error(`[طلب ${j + 1}/${ORDER_COUNT}] ⚠️ خطأ شبكة:`, err.message);
        });

      chunkPromises.push(p);
    }

    await Promise.all(chunkPromises);
    // Slight pause between burst batches
    await new Promise((r) => setTimeout(r, 150));
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log("\n=================================================");
  console.log("📊 نتائج المحاكاة النهائية:");
  console.log(`✅ إجمالي الطلبات الناجحة: ${successCount}`);
  console.log(`❌ إجمالي الطلبات الفاشلة: ${failCount}`);
  console.log(`⏱️ الوقت الإجمالي: ${durationSec} ثانية`);
  console.log("=================================================");
  console.log("💡 الآن يمكنك فتح لوحة التحكم (Admin Dashboard) وتليجرام لمشاهدة وصول الطلبات.");
}

runSimulation();

