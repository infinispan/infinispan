package org.infinispan.persistence.cluster;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class MyCustomCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<MyCustomCacheStoreConfiguration, MyCustomCacheStoreConfigurationBuilder> {

    public MyCustomCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder);
    }

    @Override
    public MyCustomCacheStoreConfigurationBuilder self() {
        return this;
    }

    @Override
    public void validate() {
    }

    @Override
    public MyCustomCacheStoreConfiguration create() {
        return new MyCustomCacheStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
    }
}
