Configuration cacheConfig = new ConfigurationBuilder().persistence()
				.addStore(RocksDBStoreConfigurationBuilder.class)
				.build();
EmbeddedCacheManager cacheManager = new DefaultCacheManager(cacheConfig);

Cache<String, User> usersCache = cacheManager.getCache("usersCache");
usersCache.put("raytsang", new User(...));
