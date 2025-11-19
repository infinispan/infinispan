// mode=local,language=javascript,parameters=[a],role=pheidippides

function process(args) {
    const cache = cacheManager.getDefaultCache();
    cacheManager.put(cache, "a", args.a);
    return cacheManager.get(cache, "a");
}
