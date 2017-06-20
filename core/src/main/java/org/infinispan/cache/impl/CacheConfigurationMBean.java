package org.infinispan.cache.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;

/**
 * CacheConfigurationMBeanImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MBean(objectName = "Configuration", description = "Runtime cache configuration attributes")
public class CacheConfigurationMBean {

    private Cache<?, ?> cache;
    private Configuration configuration;

    @Inject
    public void injectDependencies(Cache<?, ?> cache, Configuration configuration) {
        this.cache = cache;
        this.configuration = configuration;
    }

    @ManagedAttribute(description = "Gets the eviction size for the cache",
        displayName = "Gets the eviction size for the cache",
        units = Units.NONE,
        displayType = DisplayType.DETAIL, writable = true)
    public long getEvictionSize() {
        return configuration.eviction().size();
    }

    public void setEvictionSize(long newSize) {
        configuration.eviction().size(newSize);
    }
}
