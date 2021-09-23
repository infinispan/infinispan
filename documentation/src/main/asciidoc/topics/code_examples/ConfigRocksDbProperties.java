Properties props = new Properties();
props.put("database.max_background_compactions", "2");
props.put("data.write_buffer_size", "512MB");

Configuration cacheConfig = new ConfigurationBuilder().persistence()
				.addStore(RocksDBStoreConfigurationBuilder.class)
				.location("rocksdb/data")
				.expiredLocation("rocksdb/expired")
				.properties(props)
				.build();
