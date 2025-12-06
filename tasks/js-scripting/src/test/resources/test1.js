// mode=local,language=javascript

function process(args) {
    const cache = cacheManager.getCache("script-exec");
    const current = cacheManager.get(cache, "a");
    const updated = current + ":modified";
    cacheManager.put(cache, "a", updated);
    return updated;
}
