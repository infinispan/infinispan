DefaultCacheManager cacheManager = new DefaultCacheManager();

// Encode keys and values as Protobuf
ConfigurationBuilder cfg = new ConfigurationBuilder();
cfg.encoding().key().mediaType("application/x-protostream");
cfg.encoding().value().mediaType("application/x-protostream");

cacheManager.defineConfiguration("mycache", cfg.build());

Cache<Integer, Person> cache = cacheManager.getCache("mycache");

cache.put(1, new Person("John","Doe"));

// Use Protobuf for keys and JSON for values
Cache<Integer, byte[]> jsonValuesCache = (Cache<Integer, byte[]>) cache.getAdvancedCache().withMediaType("application/x-protostream", "application/json");

byte[] json = jsonValuesCache.get(1);
