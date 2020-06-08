// mode=local,language=javascript
var cache = cacheManager.getCache("script-exec");

var val = cache.get("processValue");
cache.put("processValue", val + ":additionFromJavascript");
