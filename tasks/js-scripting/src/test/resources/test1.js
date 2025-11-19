// mode=local,language=javascript

function process(user_input) {
    const cache = from_java.get_cache("script-exec");
    from_java.put(cache, "a", user_input.a + ":modified");
    return from_java.get(cache, "a");
}
