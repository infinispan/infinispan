//mode=local,language=javascript,parameters=[a, testExecWithoutProp, test]

function process(args) {
    const cache = cacheManager.getDefaultCache();
    cacheManager.put(cache, "processValue", "script1");

    const processTestExecWithoutProp = eval(`(()=>{${args.testExecWithoutProp};return process})()`);
    processTestExecWithoutProp({});

    const processTest = eval(`(()=>{${args.test};return process})()`);
    processTest(args);

    return cacheManager.get(cache, "processValue");
}
