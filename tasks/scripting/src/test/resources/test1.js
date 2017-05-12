// mode=local,language=javascript
var cache = cacheManager.getCache("script-exec");
var a = cache.get("a");

cache.put("a", a + ":modified");

cache.get("a");
