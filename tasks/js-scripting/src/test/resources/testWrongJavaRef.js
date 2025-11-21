// mode=local,language=javascript,parameters=[a]

function process(args) {
    const cache = cacheManager.getNonExistentMethod();
    cacheManager.put(cache, "a", args.a);
    return cacheManager.get(cache, "a");
}
