package org.infinispan.quarkus.embedded;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.reactivestreams.Publisher;

// Here to test a custom cache loader configured via XML
public class TestCacheLoader implements AdvancedLoadWriteStore {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public Publisher<MarshallableEntry> entryPublisher(Predicate filter, boolean fetchValue, boolean fetchMetadata) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void purge(Executor threadPool, PurgeListener listener) {

    }

    @Override
    public void init(InitializationContext ctx) {

    }

    @Override
    public void write(MarshallableEntry entry) {

    }

    @Override
    public MarshallableEntry loadEntry(Object key) {
        return null;
    }

    @Override
    public boolean delete(Object key) {
        return false;
    }

    @Override
    public boolean contains(Object key) {
        return false;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
