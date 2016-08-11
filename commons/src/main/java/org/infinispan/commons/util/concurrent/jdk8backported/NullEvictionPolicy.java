package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Collections;
import java.util.Set;

class NullEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
    private final BoundedEquivalentConcurrentHashMapV8.NodeEquivalence<K, V> nodeEq;

    public NullEvictionPolicy(BoundedEquivalentConcurrentHashMapV8.NodeEquivalence<K, V> nodeEq) {
        this.nodeEq = nodeEq;
    }

    @Override
    public void onEntryMiss(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        // Do nothing.
    }

    @Override
    public void onEntryRemove(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e) {
        // Do nothing.
    }

    @Override
    public BoundedEquivalentConcurrentHashMapV8.Node<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.Node<K, V> next, V value,
                                                                          EvictionEntry<K, V> evictionEntry) {
        // No eviction passed in
        return new BoundedEquivalentConcurrentHashMapV8.Node<K, V>(hash, nodeEq, key, value, next);
    }

    @Override
    public BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> next,
                                                                              BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry) {
        return new BoundedEquivalentConcurrentHashMapV8.TreeNode<>(hash, nodeEq, key, value, next, parent, evictionEntry);
    }

    @Override
    public Set<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> findIfEntriesNeedEvicting() {
        return Collections.emptySet();
    }

    @Override
    public void onEntryHitRead(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        // Do nothing.
    }

    @Override
    public void onEntryHitWrite(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        // Do nothing.
    }

    @Override
    public void onResize(long oldSize, long newSize) {
        // Do nothing.
    }

    @Override
    public void resize(long newSize) {
        // Do nothing.
    }
}
