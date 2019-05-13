package org.infinispan.persistence.cluster;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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

    MyCustomCacheStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
        super(attributes, async, singletonStore);
    }

    @Override
    public String toString() {
        return "MyCustomCacheStoreConfiguration{" +
              "attributes=" + attributes +
              '}';
    }
}
