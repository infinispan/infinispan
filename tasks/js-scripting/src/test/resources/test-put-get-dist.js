// mode=distributed,language=javascript,parameters=[cacheName, k, v]
var cache = cacheManager.getCache(cacheName);
cache.put(k, v);
cache.get(k);
