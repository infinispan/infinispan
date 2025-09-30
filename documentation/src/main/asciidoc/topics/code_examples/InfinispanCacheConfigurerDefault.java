@Bean
public InfinispanCacheConfigurer cacheConfigurer() {
	return manager -> {
		final Configuration ispnConfig = new ConfigurationBuilder()
                        .cacheMode(CacheMode.LOCAL)
                        .build();

		manager.defineConfiguration("default", ispnConfig);
	};
}
