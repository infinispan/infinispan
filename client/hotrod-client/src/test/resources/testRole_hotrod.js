// mode=local,language=javascript,parameters=[a],role=RWEuser
var cache = cacheManager.getCache("secured-exec");
cache.put("a", a);
cache.get("a");
