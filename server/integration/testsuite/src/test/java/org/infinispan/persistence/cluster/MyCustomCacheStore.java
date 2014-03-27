package org.infinispan.persistence.cluster;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Test cache store, which never store/loads anything (= returns null for all keys).
 *
 *  @author Jakub Markos
 */
@ConfiguredBy(MyCustomCacheStoreConfiguration.class)
public class MyCustomCacheStore implements ExternalStore {

    private static final Log log = LogFactory.getLog(MyCustomCacheStore.class);

    private MyCustomCacheStoreConfiguration config;

    @Override
    public void init(InitializationContext ctx) {
        this.config = ctx.getConfiguration();
    }

    @Override
    public void write(MarshalledEntry marshalledEntry) {
    }

    @Override
    public boolean delete(Object o) {
        return false;
    }

    @Override
    public MarshalledEntry load(Object key) throws PersistenceException {
        assert config.customProperty() == 10;
        return null;
    }

    @Override
    public boolean contains(Object key) {
        return false;
    }

    @Override
    public void start() throws PersistenceException {
        //nothing to do here
    }

    @Override
    public void stop() throws PersistenceException {
        //nothing to do here
    }

}
