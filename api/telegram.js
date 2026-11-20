/**
 * Secure Serverless Backend Endpoint: /api/telegram
 * Handles Telegram order notifications securely on the server-side.
 * 
 * SECURITY FEATURES:
 * 1. Bot token and chat ID are read from process.env (NEVER exposed to frontend).
 * 2. Strict input validation and sanitization.
 * 3. In-memory IP rate limiting (max 10 order notifications per IP per minute).
 * 4. Compatible with Vercel Serverless Functions and Node.js HTTP servers.
 */

import { enqueueTelegramNotification, WORKER_SECRET } from "./queue-worker.js";

// Simple in-memory rate limiter per IP
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const MAX_REQUESTS_PER_WINDOW = 10;

function isRateLimited(ip) {
  const now = Date.now();
  const record = rateLimitMap.get(ip) || { count: 0, resetTime: now + RATE_LIMIT_WINDOW_MS };

  if (now > record.resetTime) {
    record.count = 1;
    record.resetTime = now + RATE_LIMIT_WINDOW_MS;
  } else {
    record.count += 1;
  }

  rateLimitMap.set(ip, record);

  // Periodic cleanup of stale records
  if (rateLimitMap.size > 1000) {
    for (const [key, val] of rateLimitMap.entries()) {
      if (now > val.resetTime) rateLimitMap.delete(key);
    }
  }

  return record.count > MAX_REQUESTS_PER_WINDOW;
}

function sanitizeText(str) {
  return String(str || '')
    .replace(/[<>]/g, '')
    .trim()
    .slice(0, 200);
}

export default async function handler(req, res) {
  // Only allow POST
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // Rate limiting check
  const clientIp = req.headers?.['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown';
  if (isRateLimited(clientIp)) {
    return res.status(429).json({ error: 'Too many requests. Please wait a minute.' });
  }

  // Retrieve secrets strictly from environment
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;

  if (!token || !chatId) {
    console.error('[SECURITY ERROR] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in server environment.');
    return res.status(500).json({ error: 'Telegram notification service is not configured on the server.' });
  }

  try {
    const { orderId, orderData } = req.body || {};

    if (!orderId || !orderData) {
      return res.status(400).json({ error: 'Missing required order notification parameters.' });
    }

    // Validate and sanitize data
    const safeOrderId = sanitizeText(orderId).slice(0, 30);
    // Format items block (Supports both array of cart items and single product)
    let itemsBlock = '';
    let itemCountDesc = '1 عنصر';

    if (Array.isArray(orderData.items) && orderData.items.length > 0) {
      itemCountDesc = `${orderData.items.length} عنصر`;
      itemsBlock = orderData.items.map(it => {
        const sName = sanitizeText(it.name || it.productName || 'منتج');
        const sColor = sanitizeText(it.color || '');
        const sSize = sanitizeText(it.size || '');
        const sQty = Number(it.quantity || it.qty) || 1;
        const sPrice = Number(it.price) || 0;
        const specs = [];
        if (sSize) specs.push(`المقاس: ${sSize}`);
        if (sColor) specs.push(`اللون: ${sColor}`);
        specs.push(`الكمية: ${sQty}`);
        specs.push(`السعر: ${sPrice.toLocaleString('ar-DZ')} دج`);
        return `• ${sName}\n   - ${specs.join(' | ')}`;
      }).join('\n');
    } else {
      const safeProductName = sanitizeText(orderData.productName || 'منتج');
      const safeColor = sanitizeText(orderData.color || '');
      const safeSize = sanitizeText(orderData.size || '');
      const safeQty = Number(orderData.qty) || 1;
      const safePrice = Number(orderData.price) || 0;
      const singleSpecs = [];
      if (safeSize) singleSpecs.push(`المقاس: ${safeSize}`);
      if (safeColor) singleSpecs.push(`اللون: ${safeColor}`);
      singleSpecs.push(`الكمية: ${safeQty}`);
      singleSpecs.push(`السعر: ${safePrice.toLocaleString('ar-DZ')} دج`);
      itemsBlock = `• ${safeProductName}\n   - ${singleSpecs.join(' | ')}`;
    }

    const safeTotal = sanitizeText(orderData.total || '0');
    const safeName = sanitizeText(orderData.name || orderData.customer || 'زبون');
    const safePhone = sanitizeText(orderData.phone || '');
    const safeWilaya = sanitizeText(orderData.wilaya || '');
    const safeCommune = sanitizeText(orderData.commune || '');
    const safeAddress = sanitizeText(orderData.address || 'غير محدد');

    const messageText = [
      '🛍️ طلب جديد من متجر حجاج ',
      '==========================',
      `🔖 رقم الطلب: #${safeOrderId.slice(-6).toUpperCase()}`,
      '',
      `📦 تفاصيل المنتجات (${itemCountDesc}):`,
      itemsBlock,
      '',
      `💰 الإجمالي الكلي: ${safeTotal} دج (الدفع عند التسليم)`,
      '',
      '👤 بيانات الزبون:',
      `• الاسم: ${safeName}`,
      `• رقم الهاتف: ${safePhone}`,
      `• الولاية: ${safeWilaya}`,
      `• البلدية: ${safeCommune}`,
      `• العنوان: ${safeAddress}`,
      '',
      `📅 التاريخ: ${new Date().toLocaleString('ar-DZ')}`
    ].join('\n');

    // Enqueue message into Firestore telegramQueue
    const queuePayload = {
      orderId: safeOrderId,
      messageText,
      orderSummary: {
        customer: safeName,
        phone: safePhone,
        wilaya: safeWilaya,
        commune: safeCommune,
        deliveryMethod: 'الدفع عند التسليم',
        itemsCount: Array.isArray(orderData.items) ? orderData.items.length : 1,
        itemsText: itemCountDesc,
        total: safeTotal
      },
      status: 'pending',
      attempts: 0,
      createdAt: new Date().toISOString()
    };

    const queueRes = await enqueueTelegramNotification(queuePayload);

    // Trigger queue processor asynchronously
    const host = req.headers?.host || 'localhost';
    const proto = req.headers?.['x-forwarded-proto'] || (host.includes('localhost') ? 'http' : 'https');
    const triggerNextUrl = `${proto}://${host}/api/queue-worker`;

    const abortCtrl = new AbortController();
    const abortTimer = setTimeout(() => abortCtrl.abort(), 250);

    fetch(triggerNextUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-worker-secret': WORKER_SECRET
      },
      body: JSON.stringify({ trigger: 'telegram_fallback', orderId: safeOrderId, timestamp: Date.now() }),
      signal: abortCtrl.signal
    }).catch(() => {}).finally(() => clearTimeout(abortTimer));

    return res.status(200).json({
      success: true,
      queued: Boolean(queueRes && queueRes.success)
    });
  } catch (err) {
    console.error('[TELEGRAM HANDLER EXCEPTION]', err.message);
    return res.status(500).json({ error: 'Internal server error while processing notification.' });
  }
}
