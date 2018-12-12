package org.infinispan.persistence.cluster;

import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * Test cache store, which never store/loads anything (= returns null for all keys).
 *
 * @author Jakub Markos
 */
@ConfiguredBy(MyCustomCacheStoreConfiguration.class)
public class MyCustomCacheStore implements ExternalStore {

    private MyCustomCacheStoreConfiguration config;

    public MyCustomCacheStore() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            fail("This thread shouldn't get interrupted; " + e.getMessage());
        }
    }

    @Override
    public void init(InitializationContext ctx) {
        this.config = ctx.getConfiguration();
    }

    @Override
    public void write(MarshallableEntry marshalledEntry) {
    }

    @Override
    public boolean delete(Object o) {
        return false;
    }

    @Override
    public MarshallableEntry loadEntry(Object key) throws PersistenceException {
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
