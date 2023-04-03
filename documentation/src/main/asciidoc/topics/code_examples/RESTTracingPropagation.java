HashMap<String, String> contextMap = new HashMap<>();

// Inject the request with the *current* Context, which contains our current Span.
W3CTraceContextPropagator.getInstance().inject(Context.current(), contextMap,
(carrier, key, value) -> carrier.put(key, value));

// Pass the context map in the header
RestCacheClient client = restClient.cache(CACHE_NAME);
client.put("aaa", MediaType.TEXT_PLAIN.toString(),RestEntity.create(MediaType.TEXT_PLAIN, "bbb"), contextMap);
