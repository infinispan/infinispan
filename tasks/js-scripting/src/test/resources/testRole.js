// mode=local,language=javascript,parameters=[a],role=pheidippides
var cache = cacheManager.getCache("secured-script-exec");
cache.put("a", a);
cache.get("a");
