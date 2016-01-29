// mode=local,language=javascript
var cache = cacheManager.getCache("test_cache");

var val = cache.get("processValue");
cache.put("processValue", val + ":additionFromJavascript");