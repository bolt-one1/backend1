class InMemoryCache {
  constructor() {
    this.cache = new Map();
    this.timers = new Map();
  }

  set(key, value, ttl = 300000) {
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
    }

    this.cache.set(key, {
      value,
      timestamp: Date.now(),
      ttl
    });

    const timer = setTimeout(() => {
      this.delete(key);
    }, ttl);

    this.timers.set(key, timer);
  }

  get(key) {
    const item = this.cache.get(key);

    if (!item) {
      return null;
    }

    const now = Date.now();
    if (now - item.timestamp > item.ttl) {
      this.delete(key);
      return null;
    }

    return item.value;
  }

  has(key) {
    const item = this.cache.get(key);

    if (!item) {
      return false;
    }

    const now = Date.now();
    if (now - item.timestamp > item.ttl) {
      this.delete(key);
      return false;
    }

    return true;
  }

  delete(key) {
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
    this.cache.delete(key);
  }

  clear() {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.cache.clear();
  }

  size() {
    return this.cache.size;
  }

  getStats() {
    const stats = {
      totalEntries: this.cache.size,
      entries: []
    };

    for (const [key, item] of this.cache.entries()) {
      const age = Date.now() - item.timestamp;
      const remaining = item.ttl - age;

      stats.entries.push({
        key,
        age: Math.floor(age / 1000),
        remaining: Math.floor(remaining / 1000),
        ttl: Math.floor(item.ttl / 1000)
      });
    }

    return stats;
  }
}

const cache = new InMemoryCache();

const cacheMiddleware = (keyGenerator, ttl = 300000) => {
  return async (req, res, next) => {
    const cacheKey = typeof keyGenerator === 'function'
      ? keyGenerator(req)
      : keyGenerator;

    const cachedData = cache.get(cacheKey);

    if (cachedData) {
      return res.json(cachedData);
    }

    const originalJson = res.json.bind(res);
    res.json = (data) => {
      if (res.statusCode === 200 && data.success !== false) {
        cache.set(cacheKey, data, ttl);
      }
      return originalJson(data);
    };

    next();
  };
};

module.exports = {
  cache,
  cacheMiddleware
};
