// mode=local,language=javascript,parameters=[a],role=user
var cache = cacheManager.getCache();
cache.put("a", a);
cache.get("a");
