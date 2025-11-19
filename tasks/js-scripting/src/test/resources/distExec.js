//mode=distributed,language=javascript,parameters=[a]

function process(user_input) {
    const cache = from_java.get_default_cache();
    from_java.put(cache, "a", user_input.a);
    return from_java.address();
}
