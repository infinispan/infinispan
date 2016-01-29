// mode=local,language=javascript
var cache = cacheManager.getCache();
var a = cache.get("a");

cache.put("a", a + ":modified");

cache.get("a");
