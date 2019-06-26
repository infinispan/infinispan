Cache<?,?> secureStorageCache = cache.getAdvancedCache().withEncoding(Scrambler.class).put(k,v);
