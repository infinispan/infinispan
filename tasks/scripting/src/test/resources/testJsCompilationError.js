// mode=local,language=javascript
var cache = cacheManager.getCache();

var d = new Date();
d.setDate(d.getDate() - 5


cache.put("a", d);
cache.get("a");
