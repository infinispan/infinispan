// mode=local,language=javascript,parameters=[a]
var cache = cacheManager.getCache("script-exec");
cache.put("a", a);
cache.get("a");
