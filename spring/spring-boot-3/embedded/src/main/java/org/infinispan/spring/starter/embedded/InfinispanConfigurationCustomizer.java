package org.infinispan.spring.starter.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;

@Deprecated(forRemoval = true)
// Will be removed in future releases
@FunctionalInterface
public interface InfinispanConfigurationCustomizer {
    void customize(ConfigurationBuilder builder);
}
