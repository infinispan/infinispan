package test.org.infinispan.spring.starter.embedded.testconfiguration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfinispanCacheConfigurationTestConfiguration {
    @Autowired
    @Qualifier("base-cache")
    org.infinispan.configuration.cache.Configuration baseCache;

    @Bean(name = "small-cache")
    public org.infinispan.configuration.cache.Configuration smallCache() {
        return new ConfigurationBuilder()
            .read(baseCache)
            .memory().maxCount(1000L)
            .build();
    }

    @Bean(name = "large-cache")
    public org.infinispan.configuration.cache.Configuration largeCache() {
        return new ConfigurationBuilder()
            .read(baseCache)
            .memory().maxCount(2000L)
            .build();
    }
}
