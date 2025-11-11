var cache = cacheManager.getCache("script-exec");
cache.put("a", a);
cache.get("a");
