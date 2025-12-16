// mode=local,language=javascript,parameters=[a]

function process(args) {
    const cache = cacheManager.getCache("script-exec");
    cacheManager.put(cache, "a", args.a);
    return cacheManager.get(cache, "a");
}
