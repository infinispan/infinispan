package org.infinispan.spring.starter.remote;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

@FunctionalInterface
public interface InfinispanRemoteCacheCustomizer {
    void customize(ConfigurationBuilder builder);
}
