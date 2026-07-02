// cache-proxy/index.js
'use strict';

const { createServer, createStatsServer } = require('./server');
const redisClient = require('./redis-client');

const PORT = process.env.PROXY_PORT || 6380;
const HOST = process.env.PROXY_HOST || '127.0.0.1';

async function start() {
  await redisClient.connect();

  const server = createServer();

  server.listen(PORT, HOST, () => {
    console.log(`[proxy] TCP server listening on ${HOST}:${PORT}`);
  });

  server.on('error', (err) => {
    console.error('[proxy] server error:', err.message);
    process.exit(1);
  });

  const statsServer = createStatsServer();
  const STATS_PORT = process.env.STATS_PORT || 8080;

  statsServer.listen(STATS_PORT, HOST, () => {
    console.log(`[proxy] HTTP stats server listening on ${HOST}:${STATS_PORT}`);
  });
}

start();