// mode=local,language=javascript,parameters=[a]
var cache = cacheManager.getNonExistentMethod();
cache.put("a", a);
cache.get("a");
