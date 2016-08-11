package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Collection;

public interface EvictionPolicy<K, V> {

    BoundedEquivalentConcurrentHashMapV8.Node<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.Node<K, V> next, V value,
                                                                   EvictionEntry<K, V> evictionEntry);

    BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> next,
                                                                       BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry);

    /**
     * Invoked to notify EvictionPolicy implementation that there has been an attempt to access
     * an entry in Segment, however that entry was not present in Segment.
     * <p>
     * Note that this method is always invoked holding a lock on the table and only
     * is raised when a write operation occurs where there wasn't a previous value
     *
     * @param e accessed entry in Segment
     */
    void onEntryMiss(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value);

    /**
     * Invoked to notify EvictionPolicy implementation that an entry in Segment has been
     * accessed.
     * <p>
     * Note that this method is invoked without the lock protecting the entry and is raised
     * when there was found to be a value but it could be changed since we don't
     * hold the lock
     *
     * @param e accessed entry in Segment
     */
    void onEntryHitRead(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value);

    /**
     * Invoked to notify EvictionPolicy implementation that an entry in Segment has been
     * accessed.
     * <p>
     * Note that this method is invoked with the lock protecting the entry and is raised
     * when there is a previous value
     *
     * @param e accessed entry in Segment
     */
    void onEntryHitWrite(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value);

    /**
     * Invoked to notify EvictionPolicy implementation that an entry e has been removed from
     * Segment.
     * <p>
     * The lock will for sure be held when this invoked
     *
     * @param e removed entry in Segment
     */
    void onEntryRemove(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e);

    /**
     * This should be invoked after an operation that would cause an element to be added
     * to the map to make sure that no elements need evicting.
     * <p>
     * Note this is also invoked after a read hit.
     * <p>
     * This method is never invoked while holding a lock on any segment
     *
     * @return the nodes that were evicted
     */
    Collection<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> findIfEntriesNeedEvicting();

    void onResize(long oldSize, long newSize);

    /**
     * Invoked when resizing the container.
     *
     * @param newSize New Size applied to the container.
     */
    void resize(long newSize);
}
