package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class LRUEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
    final BoundedEquivalentConcurrentHashMapV8<K, V> map;
    final StrippedConcurrentLinkedDeque<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> deque =
          new StrippedConcurrentLinkedDeque<BoundedEquivalentConcurrentHashMapV8.Node<K, V>>();
    volatile long maxSize;
    final AtomicReference<SizeAndEvicting> currentSize = new AtomicReference<>(
          new SizeAndEvicting(0, 0));
    final EntrySizeCalculator<? super K, ? super V> sizeCalculator;
    final boolean countingMemory;

    static final long NODE_ARRAY_BASE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(BoundedEquivalentConcurrentHashMapV8.Node[].class);
    static final long NODE_ARRAY_OFFSET = Unsafe.getUnsafe().arrayIndexScale(BoundedEquivalentConcurrentHashMapV8.Node[].class);

    public LRUEvictionPolicy(BoundedEquivalentConcurrentHashMapV8<K, V> map, long maxSize,
                             EntrySizeCalculator<? super K, ? super V> sizeCalculator, boolean countingMemory) {
        this.map = map;
        this.maxSize = maxSize;
        this.sizeCalculator = sizeCalculator;
        this.countingMemory = countingMemory;
        if (countingMemory) {
            sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
            // We add the memory usage this eviction policy
            long evictionPolicySize = unsafe.ADDRESS_SIZE + unsafe.ARRAY_OBJECT_INDEX_SCALE;
            // we have 4 object references
            evictionPolicySize += unsafe.ARRAY_OBJECT_INDEX_SCALE * 4;
            // and a long and a boolean
            evictionPolicySize += 8 + 1;

            // We do a very slim approximation of how much the map itself takes up in
            // space irrespective of the elements
            long mapSize = unsafe.ADDRESS_SIZE + unsafe.ARRAY_OBJECT_INDEX_SCALE;
            // There are 2 array references to nodes
            mapSize += NODE_ARRAY_BASE_OFFSET * 2;
            // There is are 2 longs and 3 ints
            mapSize += 8 * 2 + 4 * 3;
            // Counter cell array
            mapSize += unsafe.arrayBaseOffset(BoundedEquivalentConcurrentHashMapV8.CounterCell[].class);
            // there are 8 references to other objects in the map
            mapSize += unsafe.ADDRESS_SIZE * 8;
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, BoundedEquivalentConcurrentHashMapV8.roundUpToNearest8(evictionPolicySize) +
                  BoundedEquivalentConcurrentHashMapV8.roundUpToNearest8(mapSize), 0);
        }
    }

    @Override
    public void onEntryHitRead(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
        // We synchronize in case if multiple threads are hitting this entry at the same
        // time so we don't link it last twice
        synchronized (eviction) {
            // If the queue node is null it means we just added this value
            // (but onEntryMiss hasn't ran)
            if (eviction.queueNode != null && !eviction.removed) {
                BoundedEquivalentConcurrentHashMapV8.Node<K, V> oldItem = eviction.queueNode.item;
                // Now set the item to null if possible - if we couldn't that means
                // that the entry was removed from the queue concurrently - let that win
                if (oldItem != null && eviction.queueNode.casItem(oldItem, null)) {
                    // this doesn't get unlinked if it was a tail of head here
                    deque.unlink(eviction.queueNode);

                    StrippedConcurrentLinkedDeque.DequeNode<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> queueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(e);
                    eviction.queueNode = queueNode;
                    deque.linkLast(queueNode);
                }
            }
        }
    }

    @Override
    public void onEntryHitWrite(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        onEntryHitRead(e, value);
    }

    @Override
    public void onEntryMiss(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
        synchronized (eviction) {
            if (!eviction.removed) {
                // increment size here
                StrippedConcurrentLinkedDeque.DequeNode<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> queueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(e);
                eviction.queueNode = queueNode;
                deque.linkLast(queueNode);
                BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, sizeCalculator.calculateSize(e.key, value), 0);
            }
        }
    }

    @Override
    public void onEntryRemove(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e) {
        LRUNode<K, V> eviction = (LRUNode<K, V>) e.eviction;
        synchronized (eviction) {
            if (eviction.queueNode != null) {
                BoundedEquivalentConcurrentHashMapV8.Node<K, V> item = eviction.queueNode.item;
                if (item != null && eviction.queueNode.casItem(item, null)) {
                    deque.unlink(eviction.queueNode);
                }
                eviction.queueNode = null;
            }
            // This is just in case if there are concurrent removes for the same key
            if (!eviction.removed) {
                eviction.removed = true;
                BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, -sizeCalculator.calculateSize(e.key, e.val), 0);
            }
        }
    }

    @Override
    public BoundedEquivalentConcurrentHashMapV8.Node<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.Node<K, V> next, V value,
                                                                          EvictionEntry<K, V> evictionEntry) {
        BoundedEquivalentConcurrentHashMapV8.Node<K, V> node = new BoundedEquivalentConcurrentHashMapV8.Node<K, V>(hash, map.nodeEq, key, value, next);
        if (evictionEntry == null) {
            node.lazySetEviction(new LRUNode<>(node));
        } else {
            node.lazySetEviction(evictionEntry);
        }
        return node;
    }

    @Override
    public BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> next,
                                                                              BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> parent, V value, EvictionEntry<K, V> evictionEntry) {
        BoundedEquivalentConcurrentHashMapV8.TreeNode<K, V> treeNode;
        if (evictionEntry == null) {
            treeNode = new BoundedEquivalentConcurrentHashMapV8.TreeNode<>(hash, map.nodeEq, key, value, next, parent, null);
            treeNode.lazySetEviction(new LRUNode<>(treeNode));
        } else {
            treeNode = new BoundedEquivalentConcurrentHashMapV8.TreeNode<>(hash, map.nodeEq, key, value, next, parent,
                  evictionEntry);
        }
        return treeNode;
    }

    @Override
    public Collection<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> findIfEntriesNeedEvicting() {

        long extra;
        while (true) {
            SizeAndEvicting existingSize = currentSize.get();
            long size = existingSize.size;
            long evicting = existingSize.evicting;
            // If there are extras then we need to increase eviction
            if ((extra = size - evicting - maxSize) > 0) {
                SizeAndEvicting newSize = new SizeAndEvicting(size, evicting + extra);
                if (currentSize.compareAndSet(existingSize, newSize)) {
                    break;
                }
            } else {
                break;
            }
        }
        List<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> evictedEntries = null;
        if (extra > 0) {
            evictedEntries = new ArrayList<>((int) extra & 0x7fffffff);
            long decCreate = 0;
            while (decCreate < extra) {
                BoundedEquivalentConcurrentHashMapV8.Node<K, V> node = deque.pollFirst();
                boolean removed = false;
                if (node != null) {
                    LRUNode<K, V> lruNode = (LRUNode<K, V>) node.eviction;
                    synchronized (lruNode) {
                        if (!lruNode.removed) {
                            lruNode.removed = true;
                            removed = true;
                        }
                    }
                }

                if (removed) {
                    V value = map.replaceNode(node.key, null, null, true);
                    if (value != null) {
                        evictedEntries.add(node);
                        decCreate += sizeCalculator.calculateSize(node.key, value);
                    }
                } else {
                    // This basically means there was a concurrent remove, in which case
                    // we can't know how large it was so our eviction can't be correct.
                    // In this case break out and let the next person fix the eviction
                    break;
                }
            }
            // It is possible that decCreate is higher than extra and if the size calculation
            // can return a number greater than 1 most likely to occur very often
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, -decCreate, -extra);
        } else {
            evictedEntries = Collections.emptyList();
        }

        return evictedEntries;
    }

    @Override
    public void onResize(long oldSize, long newSize) {
        if (countingMemory && newSize > oldSize) {
            // Need to increment the overall size
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, (newSize - oldSize) * NODE_ARRAY_OFFSET, 0);
        }
    }

    @Override
    public void resize(long newSize) {
        this.maxSize = newSize;
    }
}
