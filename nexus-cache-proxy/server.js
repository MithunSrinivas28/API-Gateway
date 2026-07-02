// cache-proxy/server.js
'use strict';

const net = require('net');
const http = require('http');
const { RespParser } = require('./resp/parser');
const { serialize } = require('./resp/serializer');
const redisClient = require('./redis-client');
const singleflight = require('./singleflight');
const { isAllowed } = require('../api-gateway/rateLimiter');
const { checkAndExtend, getTopKeys } = require('./hotkey');

const READ_COMMANDS = new Set([
  'GET', 'HGET', 'LRANGE', 'MGET', 'ZREVRANGE', 
  'ZRANGE', 'SMEMBERS', 'SISMEMBER', 'HGETALL', 
  'STRLEN', 'ZCARD', 'SCARD', 'HLEN'
]);
function createServer() {
  const server = net.createServer((socket) => {
    const parser = new RespParser();
    const addr = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`[proxy] client connected: ${addr}`);

    parser.on('command', async (parsed) => {
      try {
        const ip = socket.remoteAddress;
        if (!isAllowed(ip)) {
          console.log(`RATE LIMITED: ${ip}`);
          socket.write(serialize({ type: 'error', value: 'ERR rate limit exceeded' }));
          return;
        }

        // Commands arrive as RESP arrays e.g. ['SET', 'foo', 'bar']
        if (parsed.type !== 'array' || !parsed.value) {
          socket.write(serialize({ type: 'error', value: 'ERR invalid command format' }));
          return;
        }

        // Extract raw string args from bulk_string elements
        const args = parsed.value.map((el) => el.value);
        const [command, ...rest] = args;
        const cmdUpper = command.toUpperCase();

        // Track every command's key through hotkey module
        if (rest.length > 0) {
          checkAndExtend(rest[0]);
        }

        // Forward to real Redis using ioredis call method
        let result;
        if (READ_COMMANDS.has(cmdUpper)) {
          const dedupKey = args.join(':');
          result = await singleflight.do(dedupKey, () => redisClient.call(command, ...rest));
        } else {
          result = await redisClient.call(command, ...rest);
        }

        // Build response based on what ioredis returned
        let response;
        if (result === null) {
          response = { type: 'bulk_string', value: null };
        } else if (typeof result === 'number') {
          response = { type: 'integer', value: result };
        } else if (Array.isArray(result)) {
          response = {
            type: 'array',
            value: result.map((r) => ({ type: 'bulk_string', value: r })),
          };
        } else {
          response = { type: 'bulk_string', value: String(result) };
        }

        socket.write(serialize(response));
      } catch (err) {
        socket.write(serialize({ type: 'error', value: `ERR ${err.message}` }));
      }
    });

    parser.on('error', (err) => {
      console.error(`[proxy] parse error from ${addr}:`, err.message);
      socket.write(serialize({ type: 'error', value: `ERR ${err.message}` }));
      socket.destroy();
    });

    socket.on('data', (chunk) => parser.feed(chunk));

    socket.on('close', () => {
      console.log(`[proxy] client disconnected: ${addr}`);
      parser.reset();
    });

    socket.on('error', (err) => {
      console.error(`[proxy] socket error from ${addr}:`, err.message);
      parser.reset();
    });
  });

  return server;
}

function createStatsServer() {
  const statsServer = http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/hotkeys') {
      const top = getTopKeys(10);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(top, null, 2));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not Found' }));
    }
  });

  return statsServer;
}

module.exports = { createServer, createStatsServer };