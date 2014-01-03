package org.infinispan.persistence.cluster;

import java.util.Properties;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;

public class MyCustomCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<MyCustomCacheStoreConfiguration, MyCustomCacheStoreConfigurationBuilder> {
    private int customProperty;

    public MyCustomCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder);
    }

    @Override
    public MyCustomCacheStoreConfigurationBuilder self() {
        return this;
    }

    public MyCustomCacheStoreConfigurationBuilder customProperty(int customProperty) {
        this.customProperty = customProperty;
        return this;
    }

    @Override
    public MyCustomCacheStoreConfigurationBuilder withProperties(Properties p) {
        this.properties = p;
        XmlConfigHelper.setValues(this, properties, false, true);
        return this;
    }

    @Override
    public void validate() {
    }

    @Override
    public MyCustomCacheStoreConfiguration create() {
        return new MyCustomCacheStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                singletonStore.create(), preload, shared, properties, customProperty);
    }

    @Override
    public MyCustomCacheStoreConfigurationBuilder read(MyCustomCacheStoreConfiguration template) {
        this.customProperty = template.customProperty();
        this.properties = template.properties();
        return this;
    }
}