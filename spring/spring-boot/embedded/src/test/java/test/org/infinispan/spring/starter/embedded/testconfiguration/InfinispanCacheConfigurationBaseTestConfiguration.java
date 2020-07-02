package test.org.infinispan.spring.starter.embedded.testconfiguration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfinispanCacheConfigurationBaseTestConfiguration {

    @Bean(name = "base-cache")
    public org.infinispan.configuration.cache.Configuration smallCache() {
        return new ConfigurationBuilder()
            .simpleCache(true)
            .memory().size(500L)
            .memory().evictionType(EvictionType.COUNT)
            .build();
    }
}
