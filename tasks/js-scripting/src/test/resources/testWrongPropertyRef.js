// mode=local,language=javascript
var cache = cacheManager.getCache("script-exec");

var d = new Date();
d.setDate(d.getDate() - 5);


cache.put("a", a);
cache.get("a");
