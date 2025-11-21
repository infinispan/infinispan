// mode=local,language=javascript

function process(args) {
    const cache = cacheManager.getCache("script-exec");
    var val = cacheManager.get(cache, "processValue");
    cacheManager.put(cache, "processValue", val + ":additionFromJavascript");
    return val;
}