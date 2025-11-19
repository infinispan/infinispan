//mode=local,language=javascript,parameters=[a, testExecWithoutProp, test]

function process(user_input) {
    const cache = from_java.get_default_cache();
    from_java.put(cache, "processValue", "script1");

    const processTestExecWithoutProp = eval(`(()=>{${user_input.testExecWithoutProp};return process})()`);
    processTestExecWithoutProp({});

    const processTest = eval(`(()=>{${user_input.test};return process})()`);
    processTest(user_input);

    return from_java.get(cache, "processValue");
}
