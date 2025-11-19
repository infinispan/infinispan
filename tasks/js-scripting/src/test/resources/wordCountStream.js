// mode=local,language=javascript

function process(user_input, cache) {
    const cacheEntries = from_java.entry_set(from_java.get_default_cache());

    return cacheEntries
      .map(e => Object.values(e)[0])
      .map(v => String(v).toLowerCase())
      .map(v => v.split(/\W+/).filter(w => w.length > 0))
      .reduce((allWords, arr) => allWords.concat(arr), [])
      .reduce((acc, word) => {
        acc[word] = (acc[word] || 0) + 1;
        return acc;
      }, {});
}
