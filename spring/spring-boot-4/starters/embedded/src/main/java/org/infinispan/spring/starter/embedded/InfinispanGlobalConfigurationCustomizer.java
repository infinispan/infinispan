package org.infinispan.spring.starter.embedded;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;

@FunctionalInterface
public interface InfinispanGlobalConfigurationCustomizer {
    void customize(GlobalConfigurationBuilder builder);
}
