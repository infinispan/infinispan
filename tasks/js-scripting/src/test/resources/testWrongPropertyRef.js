// mode=local,language=javascript

function process(args) {
    const cache = cacheManager.getCache("script-exec");

    var d = new Date();
    d.setDate(d.getDate() - 5);

    cacheManager.put(cache, "a", args.a);
    return cacheManager.get(cache, "a");
}
