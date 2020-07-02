@Bean
public InfinispanGlobalConfigurationCustomizer globalCustomizer() {
   return builder -> builder.transport().clusterName(CLUSTER_NAME);
}

@Bean
public InfinispanConfigurationCustomizer configurationCustomizer() {
   return builder -> builder.memory().evictionType(EvictionType.COUNT);
}
