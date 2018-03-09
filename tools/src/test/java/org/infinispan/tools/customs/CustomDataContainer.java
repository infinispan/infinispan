package org.infinispan.tools.customs;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

/**
 * Custom DataContainer for testing the configuration converter.
 *
 * @author amanukya
 */
public class CustomDataContainer implements DataContainer {
    @Override
    public InternalCacheEntry get(Object k) {
        return null;
    }

    @Override
    public InternalCacheEntry peek(Object k) {
        return null;
    }

    @Override
    public void put(Object o, Object o2, Metadata metadata) {

    }

    @Override
    public boolean containsKey(Object k) {
        return false;
    }

    @Override
    public InternalCacheEntry remove(Object k) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int sizeIncludingExpired() {
        return 0;
    }

    @Override
    public int sizeIncludingExpiredAndTombstones() {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public Set keySet() {
        return null;
    }

    @Override
    public Collection values() {
        return null;
    }

    @Override
    public Set<InternalCacheEntry> entrySet() {
        return null;
    }

    @Override
    public void evict(Object key) {

    }

    @Override
    public InternalCacheEntry compute(Object key, ComputeAction action) {
        return null;
    }

    @Override
    public Iterator<InternalCacheEntry> iterator() {
        return null;
    }

    @Override
    public Iterator<InternalCacheEntry> iteratorIncludingExpired() {
        return null;
    }

    @Override
    public Iterator<InternalCacheEntry> iteratorIncludingExpiredAndTombstones() {
        return null;
    }

   @Override
    public void executeTask(KeyValueFilter filter, BiConsumer action) throws InterruptedException {

    }

    @Override
    public void executeTask(KeyFilter filter, BiConsumer action) throws InterruptedException {

    }
}
