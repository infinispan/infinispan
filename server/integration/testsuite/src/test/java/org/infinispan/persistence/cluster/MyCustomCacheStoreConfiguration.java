package org.infinispan.persistence.cluster;

import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

/**
 * Test configuration for MyCustomCacheStore.
 * Copy of ClusterLoaderConfiguration with a new customProperty.
 *
 * @author Jakub Markos
 */
@ConfigurationFor(MyCustomCacheStore.class)
@BuiltBy(MyCustomCacheStoreConfigurationBuilder.class)
public class MyCustomCacheStoreConfiguration extends AbstractStoreConfiguration {

    private int customProperty;

    public MyCustomCacheStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                       boolean ignoreModifications, AsyncStoreConfiguration async,
                                       SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties,
                                       int customProperty) {
        super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
        this.customProperty = customProperty;
    }

    public int customProperty() {
        return customProperty;
    }

    @Override
    public String toString() {
        return "MyCustomCacheStoreConfiguration [customProperty=" + customProperty + "]";
    }
}
