package org.infinispan.persistence.cluster;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
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

    private Marshaller clientMarshaller;
    private StreamingMarshaller serverMarshaller;
    private StoreConfiguration config;

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
        this.clientMarshaller = new GenericJBossMarshaller();
        this.serverMarshaller = ctx.getMarshaller();
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
        if (!(key instanceof WrappedByteArray))
            throw new IllegalArgumentException(String.format("Expected key to be of type '%s'", WrappedByteArray.class.getSimpleName()));

        WrappedByteArray wrappedKey = (WrappedByteArray) key;
        try {
            String unwrappedKey = (String) clientMarshaller.objectFromByteBuffer(wrappedKey.getBytes());
            String propertyValue = config.properties().getProperty(unwrappedKey);
            if (propertyValue == null)
                return null;
            WrappedByteArray wrappedValue = new WrappedByteArray(clientMarshaller.objectToByteBuffer(propertyValue));
            return new MarshalledEntryImpl<>(key, wrappedValue, null, serverMarshaller);
        } catch (ClassNotFoundException | IOException e) {
            throw new IllegalStateException(e);
        }
        catch (InterruptedException e) {
            throw new AssertionError(e);
        }
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
