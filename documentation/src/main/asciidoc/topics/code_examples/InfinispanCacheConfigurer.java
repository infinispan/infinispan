@Bean
public InfinispanCacheConfigurer cacheConfigurer() {
	return manager -> {
		final Configuration ispnConfig = new ConfigurationBuilder()
                        .clustering()
                        .cacheMode(CacheMode.LOCAL)
                        .build();

		manager.defineConfiguration("local-sync-config", ispnConfig);
	};
}
