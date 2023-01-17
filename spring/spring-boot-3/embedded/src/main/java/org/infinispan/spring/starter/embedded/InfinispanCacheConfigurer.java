package org.infinispan.spring.starter.embedded;

import org.infinispan.manager.EmbeddedCacheManager;

@FunctionalInterface
public interface InfinispanCacheConfigurer {
    /**
     * Configure an Infinispan cache.
     *
     * @param manager The {@link EmbeddedCacheManager}.
     */
    void configureCache(final EmbeddedCacheManager manager);
}
