// mode=local,language=javascript

function process(user_input) {
    const cache = from_java.get_cache("script-exec");
    var val = from_java.get(cache, "processValue");
    from_java.put(cache, "processValue", val + ":additionFromJavascript");
    return val;
}