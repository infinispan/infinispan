// mode=local,language=javascript

function process(user_input) {
    const cache = from_java.get_cache("script-exec");
    const current = from_java.get(cache, "a");
    const updated = current + ":modified";
    from_java.put(cache, "a", updated);
    return updated;
}
