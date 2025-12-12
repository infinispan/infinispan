// mode=local,language=javascript,parameters=[a],role=pheidippides

function process(args) {
    const cache = cacheManager.getCache("secured-script-exec");
    cacheManager.put(cache, "a", args.a);
    return cacheManager.get(cache, "a");
}

