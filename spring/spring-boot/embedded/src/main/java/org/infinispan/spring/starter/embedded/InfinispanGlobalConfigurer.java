package org.infinispan.spring.starter.embedded;

import org.infinispan.configuration.global.GlobalConfiguration;

@FunctionalInterface
public interface InfinispanGlobalConfigurer {
    GlobalConfiguration getGlobalConfiguration();
}
