// cache-proxy/hotkey.js
'use strict';

const redisClient = require('./redis-client');

const HOT_THRESHOLD = 10;     // hits before key is considered hot
const TTL_EXTENSION = 120;    // extra seconds added to hot key TTL

const hitCounts = new Map();

function trackHit(key) {
  const count = (hitCounts.get(key) || 0) + 1;
  hitCounts.set(key, count);
  return count;
}

async function checkAndExtend(key) {
  const count = trackHit(key);

  if (count >= HOT_THRESHOLD) {
    const ttl = await redisClient.ttl(key);

    // Only extend if key has a TTL set (ttl > 0)
    if (ttl > 0) {
      await redisClient.expire(key, ttl + TTL_EXTENSION);
      console.log(`[hotkey] ${key} is hot (${count} hits) — TTL extended by ${TTL_EXTENSION}s`);
    }
  }
}

function getHitCount(key) {
  return hitCounts.get(key) || 0;
}

function getTopKeys(n = 10) {
  return [...hitCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([key, hits]) => ({ key, hits }));
}

module.exports = { checkAndExtend, getHitCount, getTopKeys };