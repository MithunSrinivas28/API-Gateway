// cache-proxy/singleflight.js
'use strict';

class Singleflight {
  constructor() {
    this._inflight = new Map();
  }

  async do(key, fn) {
    if (this._inflight.has(key)) {
      console.log(`SINGLEFLIGHT HIT: ${key}`);
      // Already in-flight — wait for the same promise
      return this._inflight.get(key);
    }

    const promise = fn().finally(() => {
      this._inflight.delete(key);
    });

    this._inflight.set(key, promise);
    return promise;
  }
}

module.exports = new Singleflight();