// mode=distributed,language=javascript,parameters=[a],role=runner

function process(args) {
    const cache = cacheManager.getDefaultCache();
    cacheManager.put(cache, "a", args.a);
    return cacheManager.address();
}
