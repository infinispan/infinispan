Cache<String, String> cache = cacheManager.createCache("namedCache",
      new MutableConfiguration<String, String>().setStoreByValue(false));
