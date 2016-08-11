package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

final class LIRSEvictionPolicy<K, V> implements EvictionPolicy<K, V> {
    /**
     * The percentage of the cache which is dedicated to hot blocks. See section 5.1
     */
    private static final float L_LIRS = 0.95f;

    enum Recency {
        HIR_RESIDENT, LIR_RESIDENT, HIR_NONRESIDENT, EVICTING, EVICTED, REMOVED
    }

    final BoundedEquivalentConcurrentHashMapV8<K, V> map;
    // LIRS stack S
    /**
     * The LIRS stack, S, which is maintains recency information. All hot entries are on the
     * stack. All cold and non-resident entries which are more recent than the least recent hot
     * entry are also stored in the stack (the stack is always pruned such that the last entry is
     * hot, and all entries accessed more recently than the last hot entry are present in the
     * stack). The stack is ordered by recency, with its most recently accessed entry at the top,
     * and its least recently accessed entry at the bottom.
     */
    final StrippedConcurrentLinkedDeque<LIRSNode<K, V>> stack = new StrippedConcurrentLinkedDeque<>();
    // LIRS queue Q
    /**
     * The LIRS queue, Q, which enqueues all cold entries for eviction. Cold entries (by
     * definition in the queue) may be absent from the stack (due to pruning of the stack). Cold
     * entries are added to the end of the queue and entries are evicted from the front of the
     * queue.
     */
    final StrippedConcurrentLinkedDeque<LIRSNode<K, V>> queue = new StrippedConcurrentLinkedDeque<>();

    /**
     * The maximum number of hot entries (L_lirs in the paper).
     */
    private volatile long maximumHotSize;

    /**
     * The maximum number of resident entries (L in the paper).
     */
    private volatile long maximumSize;

    /**
     * The actual number of hot entries.
     */
    private final AtomicLong hotSize = new AtomicLong();

    /**
     * This stores how many LIR to HIR demotions we need to do to keep in a consistent
     * state.  Note this should only be incremented when you are promoting a HIR to
     * LIR usually.
     */
    private final AtomicLong hotDemotion = new AtomicLong();

    private final AtomicReference<SizeAndEvicting> currentSize = new AtomicReference<>(
          new SizeAndEvicting(0, 0));

    final ThreadLocal<Collection<LIRSNode<K, V>>> nodesToEvictTL = new ThreadLocal<>();

    public LIRSEvictionPolicy(BoundedEquivalentConcurrentHashMapV8<K, V> map, long maxSize) {
        this.map = map;
        this.maximumSize = maxSize;
        this.maximumHotSize = calculateLIRSize(maxSize);
    }

    private static long calculateLIRSize(long maximumSize) {
        long result = (long) (L_LIRS * maximumSize);
        return (result == maximumSize) ? maximumSize - 1 : result;
    }

    @Override
    public BoundedEquivalentConcurrentHashMapV8.Node<K, V> createNewEntry(K key, int hash, BoundedEquivalentConcurrentHashMapV8.Node<K, V> next, V value,
                                                                          EvictionEntry<K, V> evictionEntry) {
        BoundedEquivalentConcurrentHashMapV8.Node<K, V> node = new BoundedEquivalentConcurrentHashMapV8.Node<K, V>(hash, map.nodeEq, key, value, next);
        if (evictionEntry == null) {
            node.lazySetEviction(new LIRSNode<K, V>(key));
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
            treeNode.lazySetEviction(new LIRSNode<K, V>(key));
        } else {
            // We need to link the eviction entry with the new node now too
            treeNode = new BoundedEquivalentConcurrentHashMapV8.TreeNode<>(hash, map.nodeEq, key, value, next, parent, evictionEntry);
        }
        return treeNode;
    }

    /**
     * Adds this LIRS node as LIR if there is room.
     * The lock must be obtained on the node to ensure consistency
     *
     * @param lirsNode The node to try to add
     * @return if the node was added or not
     */
    boolean addToLIRIfNotFullHot(LIRSNode<K, V> lirsNode, boolean incrementSize) {
        long currentHotSize;
        // If we haven't hit LIR max then promote it immediately
        // See section 3.3 second paragraph:
        while ((currentHotSize = hotSize.get()) < maximumHotSize) {
            if (hotSize.compareAndSet(currentHotSize, currentHotSize + 1)) {
                if (incrementSize) {
                    BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, 1, 0);
                }
                StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                lirsNode.setStackNode(stackNode);
                lirsNode.setState(Recency.LIR_RESIDENT);
                stack.linkLast(stackNode);
                return true;
            }
        }
        return false;
    }

    @Override
    public void onEntryMiss(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        long pruneLIR = 0;
        boolean evictHIR = false;
        boolean skipIncrement;
        LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
        synchronized (lirsNode) {
            // This tells us if this is the first time we hit a miss for this node.
            // This is important to tell if a node was hit and subsequently evicted
            // before our miss occurred
            skipIncrement = lirsNode.created;
            // Now set it to created
            lirsNode.created = true;
            Recency recency = lirsNode.state;
            // See section 3.3 case 3:
            if (recency == null) {
                if (skipIncrement) {
                    throw new IllegalStateException("Created should always be false for a"
                          + "newly created evictio node!");
                }

                long hotDifference;
                // This should be the most common case by far
                // (alreadyCreated is implied to be false)
                // If it was added to LIR due to size don't do anymore
                if (addToLIRIfNotFullHot(lirsNode, true)) {
                    return;
                } else if ((hotDifference = hotSize.get() - maximumHotSize) > 0) {
                    // This can only happen if we had a resize where the new size is less than the hot max size
                    pruneLIR = hotDifference;
                }

                // This is the (b) example
                lirsNode.setState(Recency.HIR_RESIDENT);
                // We have to add it to the stack before the queue in case if it
                // got removed by another miss
                StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                lirsNode.setStackNode(stackNode);
                stack.linkLast(stackNode);
                StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                lirsNode.setQueueNode(queueNode);
                queue.linkLast(queueNode);
            } else {
                // If this was true it means this node was not created by this miss
                // (essentially means the value was changed to HIR_NONRESIDENT or is being
                // EVICTED soon
                if (skipIncrement) {
                    switch (recency) {
                        case HIR_NONRESIDENT:
                            // This is the 3.3 case 3 (a) example
                            e.val = value;
                            // We don't add a value to the map here as the map implementation
                            // will handle this for us afterwards

                            // If it was added to LIR due to size don't do anymore
                            if (addToLIRIfNotFullHot(lirsNode, true)) {
                                return;
                            }

                            // This is the (a) example
                            promoteHIRToLIR(lirsNode);
                            pruneLIR = 1;
                            // Since we are adding back in a value we set it to increment
                            skipIncrement = false;
                            break;
                        case EVICTING:
                            e.val = value;
                            // We don't add a value to the map here as the map implementation
                            // will handle this for us afterwards

                            // In the case of eviction we add the node back as if it was a HIR_RESIDENT
                            // except we also have to evict an old HIR to make room
                            evictHIR = true;
                            lirsNode.setState(Recency.HIR_RESIDENT);
                            // We have to add it back now
                            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                            lirsNode.setStackNode(stackNode);
                            stack.linkLast(stackNode);
                            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                            lirsNode.setQueueNode(queueNode);
                            queue.linkLast(queueNode);
                            break;
                        case REMOVED:
                        case EVICTED:
                            // It cannot turn into a resident since it would require a put to
                            // update the value to non null, which would require holding the lock
                            // Both Removed and Evicted hold the lock during the entire duration
                            // so neither state should be possible as they will remove node
                            throw new IllegalStateException("Cannot have a miss on a key and then "
                                  + "get a node in " + recency);
                        case HIR_RESIDENT:
                        case LIR_RESIDENT:
                            // Ignore any entry in this state as it is already updated
                            break;
                    }
                }
            }
        }

        if (pruneLIR > 0) {
            hotDemotion.addAndGet(pruneLIR);
        }

        // Note only 1 of these can be true
        if (!skipIncrement || evictHIR) {
            // The size is checked in the findIfEntriesNeedEvicting
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, 1, 0);
        }
    }

    @SuppressWarnings("unchecked")
    void demoteLowestLIR() {
        boolean pruned = false;
        while (!pruned) {
            // Now we prune to make room for our promoted node
            Object[] LIRDetails = pruneIncludingLIR();
            if (LIRDetails == null) {
                // There was nothing to prune!
                return;
            } else {
                StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> removedDequeNode = (StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>>) LIRDetails[0];
                LIRSNode<K, V> removedLIR = (LIRSNode<K, V>) LIRDetails[1];
                synchronized (removedLIR) {
                    if (removedDequeNode != removedLIR.stackNode) {
                        continue;
                    }
                    // If the node was removed concurrently removed we ignore it and get the
                    // next one still
                    if (removedLIR.state != Recency.REMOVED) {
                        // If the stack node is still the one we removed, then we can continue
                        // with demotion.  If not then we had a concurrent hit which resurrected
                        // the LIR so we pick the next one to evict

                        // We demote the LIR_RESIDENT to HIR_RESIDENT in the queue (not in stack)
                        removedLIR.setState(Recency.HIR_RESIDENT);
                        removedLIR.setStackNode(null);
                        StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(removedLIR);
                        removedLIR.setQueueNode(queueNode);
                        queue.linkLast(queueNode);
                        pruned = true;
                    }
                }
            }
        }
    }

    /**
     * Prunes blocks in the bottom of the stack until a HOT block is removed.
     * If pruned blocks were resident, then they remain in the queue; non-resident blocks (if any)
     * are dropped
     *
     * @return Returns an array storing the removed LIR details.  The first element is the DequeNode that was removed
     * from the stack deque - this is helpful to determine if this entry was update concurrently (because it will have
     * a new stack deque pointer if it was).  The second element is the actual Node that this is tied to value wise
     */
    @SuppressWarnings("unchecked")
    Object[] pruneIncludingLIR() {
        // See section 3.3:
        // "We define an operation called "stack pruning" on the LIRS
        // stack S, which removes the HIR blocks in the bottom of
        // the stack until an LIR block sits in the stack bottom. This
        // operation serves for two purposes: (1) We ensure the block in
        // the bottom of the stack always belongs to the LIR block set.
        // (2) After the LIR block in the bottom is removed, those HIR
        // blocks contiguously located above it will not have chances to
        // change their status from HIR to LIR, because their recencies
        // are larger than the new maximum recency of LIR blocks."
        /**
         * Note that our implementation is done lazily and we don't prune HIR blocks
         * until we know we are removing a LIR block.  The reason for this is that
         * we can't do a head CAS and only can poll from them
         * WARNING: This could cause a HIR block that should have been pruned to be
         * promoted to LIR if it is accesssed before another LIR block is promoted.
         * Unfortunately there is not an easier way to do this without adding additional
         * contention.  Need to figure out if the contention would be high or not
         */
        LIRSNode<K, V> removedLIR = null;
        Object[] nodeDetails = new Object[2];
        while (true) {
            if (!stack.pollFirstNode(nodeDetails)) {
                long hot;
                // If hot size is less than 0 that means we had a concurrent removal of
                // the last contents in the cache, so we need to make sure to not to loop
                // waiting for a value
                while ((hot = hotSize.get()) < 0) {
                    if (hotSize.compareAndSet(hot, hot + 1)) {
                        return null;
                    }
                }
                continue;
            }
            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> removedStackNode = (StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>>) nodeDetails[0];
            removedLIR = (LIRSNode<K, V>) nodeDetails[1];
            synchronized (removedLIR) {
                if (removedStackNode != removedLIR.stackNode) {
                    continue;
                }
                switch (removedLIR.state) {
                    case LIR_RESIDENT:
                        // Note we don't null out the stack because the caller once again
                        // does a check to make sure the stack pointer hasn't changed
                        return nodeDetails;
                    case HIR_NONRESIDENT:
                        // Non resident was already evicted and now it is no longer in the
                        // queue or the stack so it is effectively gone - however we want to
                        // remove the now null node
                        removedLIR.setState(Recency.EVICTING);
                        Collection<LIRSNode<K, V>> nodesToEvict = nodesToEvictTL.get();
                        if (nodesToEvict == null) {
                            nodesToEvict = new ArrayList<>();
                            nodesToEvictTL.set(nodesToEvict);
                        }
                        nodesToEvict.add(removedLIR);
                    case HIR_RESIDENT:
                        // Leave it in the queue if it was a resident
                        removedLIR.setStackNode(null);
                        break;
                    case REMOVED:
                    case EVICTING:
                    case EVICTED:
                        // We ignore this value if it it was evicted/removed elsewhere as it is
                        // no longer LIRS
                        break;
                }
            }
        }
    }

    /**
     * The node must be locked before calling this method
     *
     * @param lirsNode
     */
    void promoteHIRToLIR(LIRSNode<K, V> lirsNode) {
        // This block first unlinks the node from both the stack and queue before
        // repositioning it
        {
            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
            // Stack node could be null if this node was pruned in demotion concurrently
            if (stackNode != null) {
                LIRSNode<K, V> item = stackNode.item;
                if (item != null && stackNode.casItem(item, null)) {
                    stack.unlink(stackNode);
                }
                lirsNode.setStackNode(null);
            }

            // Also unlink from queue node if it was set
            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode = lirsNode.queueNode;
            if (queueNode != null) {
                LIRSNode<K, V> item = queueNode.item;
                if (item != null && queueNode.casItem(item, null)) {
                    queue.unlink(queueNode);
                }
                lirsNode.setQueueNode(null);
            }
        }

        // Promoting the node to LIR and add to the stack
        lirsNode.setState(Recency.LIR_RESIDENT);
        StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
        lirsNode.setStackNode(stackNode);
        stack.linkLast(stackNode);
    }

    @Override
    public void onEntryHitRead(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        boolean reAttempt = false;
        LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
        synchronized (lirsNode) {
            Recency recency = lirsNode.state;
            // If the recency is non resident or evicting that means
            // we may have to do a write to the value most likely, so we retry this
            // with the table lock so we can properly do the update - NOTE that the
            // recency can change outside of this lock
            if (recency == Recency.HIR_NONRESIDENT ||
                  recency == Recency.EVICTING) {
                reAttempt = true;
            } else {
                onEntryHitWrite(e, value);
            }
        }
        if (reAttempt) {
            int hash = BoundedEquivalentConcurrentHashMapV8.spread(map.keyEq.hashCode(lirsNode.getKey())); // EQUIVALENCE_MOD
            for (BoundedEquivalentConcurrentHashMapV8.Node<K, V>[] tab = map.table; ; ) {
                BoundedEquivalentConcurrentHashMapV8.Node<K, V> f;
               int n, i;
                if (tab == null || (n = tab.length) == 0 ||
                      (f = BoundedEquivalentConcurrentHashMapV8.tabAt(tab, i = (n - 1) & hash)) == null)
                    break;
                else if (f.hash == BoundedEquivalentConcurrentHashMapV8.MOVED)
                    tab = map.helpTransfer(tab, f);
                else {
                    synchronized (f) {
                        if (BoundedEquivalentConcurrentHashMapV8.tabAt(tab, i) == f) {
                            synchronized (lirsNode) {
                                onEntryHitWrite(e, value);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    @Override
    public void onEntryHitWrite(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e, V value) {
        boolean demoteLIR = false;
        boolean evictHIR = false;
        LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
        synchronized (lirsNode) {
            // Section 3.3
            //
            Recency recency = lirsNode.state;
            // If the state is still null that means it was added and we got in
            // before the onEntryMiss was fired, so that means this is automatically
            // a LIR resident
            if (recency == null) {
                // If it was added to LIR don't need anymore work
                if (addToLIRIfNotFullHot(lirsNode, false)) {
                    return;
                }
                recency = Recency.LIR_RESIDENT;
                lirsNode.setState(recency);
                // If we are doing a promotion of HIR to LIR we need to do pruning as well
                // Remember if recency was null that means we just had a concurrent
                // onEntryMiss but it hasn't been processed yet, which means it would
                // been a HIR resident
                demoteLIR = true;
            }

            switch (recency) {
                case LIR_RESIDENT:
                    // case 1
                    //
                    // Note that if we had a concurrent pruning targeting this node, getting
                    // a hit takes precedence
                    StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
                    // This will be null if we got in before onEntryMiss
                    if (stackNode != null) {
                        LIRSNode<K, V> item = stackNode.item;
                        if (item != null && stackNode.casItem(item, null)) {
                            stack.unlink(stackNode);
                        }
                    }
                    // Now that we have it removed promote it to the top
                    StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> newStackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                    lirsNode.setStackNode(newStackNode);
                    stack.linkLast(newStackNode);
                    break;
                case HIR_NONRESIDENT:
                    if (e.val == BoundedEquivalentConcurrentHashMapV8.NULL_VALUE) {
                        // We essentially added the value back in so we have to set the value
                        // and increment the count
                        e.val = value;
                        map.addCount(1, -1);
                    }
                    // Non resident can happen if we have a hit and then a concurrent HIR pruning
                    // caused our node to be non resident.
                    if (addToLIRIfNotFullHot(lirsNode, true)) {
                        return;
                    }
                    promoteHIRToLIR(lirsNode);
                    // The only way this is possible is if we had a concurrent eviction
                    // of the key right after we saw it existed but before we could
                    // act on it.  Since we revived it to LIR we also have to set it's
                    // value to what it should be
                    evictHIR = true;
                    demoteLIR = true;
                    break;
                case EVICTED:
                    // We can't reliably do a put here without having a point where the object
                    // was possibly seen as null so we leave it as null to be more consistent
                    break;
                case EVICTING:
                    // In the case of eviction we add the node back as if it was a HIR_RESIDENT
                    // except we also have to evict an old HIR to make room
                    // Note the stackNode is assumed to be null in this case
                    evictHIR = true;
                    lirsNode.setState(Recency.HIR_RESIDENT);
                    // It is possible the node trying to be evicted was a NON resident before
                    // In that case we set the value and increment the count in the map
                    if (e.val == BoundedEquivalentConcurrentHashMapV8.NULL_VALUE) {
                        e.val = value;
                        map.addCount(1, -1);
                    }
                    // Note this falls through, since we saved the evicting entry it is like
                    // we now have a HIR resident
                case HIR_RESIDENT:
                    // case 2
                    if (lirsNode.stackNode != null) {
                        // This is the (a) example
                        //
                        // In the case it was in the stack we promote it
                        promoteHIRToLIR(lirsNode);
                        // Need to demote a LIR to make room
                        demoteLIR = true;
                    } else {
                        // This is the (b) example
                        //
                        // In the case it wasn't in the stack but was in the queue, we
                        // add it again to the stack and bump it up to the top of the queue
                        if (lirsNode.queueNode != null) {
                            LIRSNode<K, V> item = lirsNode.queueNode.item;
                            if (item != null && lirsNode.queueNode.casItem(item, null)) {
                                queue.unlink(lirsNode.queueNode);
                            }
                        }

                        newStackNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                        lirsNode.setStackNode(newStackNode);
                        stack.linkLast(newStackNode);
                        StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> newQueueNode = new StrippedConcurrentLinkedDeque.DequeNode<>(lirsNode);
                        lirsNode.setQueueNode(newQueueNode);
                        queue.linkLast(newQueueNode);
                    }
                    break;
                case REMOVED:
                    // If the entry was removed we ignore the hit.
                    break;
            }
        }
        if (demoteLIR) {
            hotDemotion.incrementAndGet();
        }
        if (evictHIR) {
            // The size is checked in the findIfEntriesNeedEvicting
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, 1, 0);
        }
    }

    @Override
    public void onEntryRemove(BoundedEquivalentConcurrentHashMapV8.Node<K, V> e) {
        LIRSNode<K, V> lirsNode = (LIRSNode<K, V>) e.eviction;
        synchronized (lirsNode) {
            switch (lirsNode.state) {
                case LIR_RESIDENT:
                    hotSize.decrementAndGet();
                case HIR_RESIDENT:
                    BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, -1, 0);
                case HIR_NONRESIDENT:
                case EVICTING:
                    // In the case of eviction/non resident we already subtracted the value
                    // And we don't want to remove twice
                    lirsNode.setState(Recency.REMOVED);
                    break;
                case REMOVED:
                case EVICTED:
                    // This shouldn't be possible
            }

            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode = lirsNode.queueNode;
            if (queueNode != null) {
                LIRSNode<K, V> item = queueNode.item;
                if (item != null && queueNode.casItem(item, null)) {
                    queue.unlink(queueNode);
                }
                lirsNode.setQueueNode(null);
            }
            lirsNode.setQueueNode(null);
            StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode = lirsNode.stackNode;
            if (stackNode != null) {
                LIRSNode<K, V> item = stackNode.item;
                if (item != null && stackNode.casItem(item, null)) {
                    stack.unlink(stackNode);
                }
                lirsNode.setStackNode(null);
            }
        }
    }

    @Override
    public Collection<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> findIfEntriesNeedEvicting() {
        long hotDemotions;
        while ((hotDemotions = hotDemotion.get()) > 0) {
            if (hotDemotion.compareAndSet(hotDemotions, 0)) {
                break;
            }
        }
        for (long i = 0; i < hotDemotions; ++i) {
            demoteLowestLIR();
        }
        int evictCount;
        while (true) {
            SizeAndEvicting sizeEvict = currentSize.get();
            long size = sizeEvict.size;
            long evict = sizeEvict.evicting;
            long longEvictCount = (size - evict - maximumSize);
            if (longEvictCount > 0) {
                evictCount = (int) longEvictCount & 0x7fffffff;
                if (currentSize.compareAndSet(sizeEvict,
                      new SizeAndEvicting(size, evict + evictCount))) {
                    break;
                }
            } else {
                evictCount = 0;
                break;
            }
        }
        // If this is non null it is also non empty
        Collection<LIRSNode<K, V>> tlEvicted = nodesToEvictTL.get();
        if (tlEvicted == null) {
            tlEvicted = Collections.emptyList();
        } else {
            nodesToEvictTL.remove();
        }
        if (evictCount != 0 || !tlEvicted.isEmpty()) {
            @SuppressWarnings("unchecked")
            LIRSNode<K, V>[] queueContents = new LIRSNode[evictCount + tlEvicted.size()];
            Iterator<LIRSNode<K, V>> tlIterator = tlEvicted.iterator();
            int offset = 0;
            while (tlIterator.hasNext()) {
                queueContents[evictCount + offset] = tlIterator.next();
                offset++;
            }
            int evictedValues = evictCount;
            int decEvict = evictCount;
            Object[] hirDetails = new Object[2];
            for (int i = 0; i < evictCount; ++i) {
                boolean foundNode = false;
                while (!foundNode) {
                    if (!queue.pollFirstNode(hirDetails)) {
                        SizeAndEvicting sizeEvict = currentSize.get();
                        // If the size was changed behind our back by a remove we
                        // need to detect that
                        if (sizeEvict.size - sizeEvict.evicting < maximumSize) {
                            SizeAndEvicting newSizeEvict = new SizeAndEvicting(sizeEvict.size,
                                  sizeEvict.evicting - 1);
                            if (currentSize.compareAndSet(sizeEvict, newSizeEvict)) {
                                evictedValues--;
                                decEvict--;
                                break;
                            }
                        }
                        // This could be valid in the case when an entry is promoted to LIR and
                        // hasn't yet moved the demoted LIR to HIR
                        // We loop back around to get this - this could spin loop if it takes
                        // the other thread long to add the HIR element back
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        continue;
                    }
                    StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> removedDequeNode = (StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>>) hirDetails[0];
                    LIRSNode<K, V> removedHIR = (LIRSNode<K, V>) hirDetails[1];
                    synchronized (removedHIR) {
                        if (removedHIR.queueNode != removedDequeNode) {
                            continue;
                        }
                        // Since we removed the queue node we need to null it out
                        removedHIR.setQueueNode(null);
                        switch (removedHIR.state) {
                            case HIR_RESIDENT:
                                // If it is in the stack we set it to HIR non resident
                                if (removedHIR.stackNode != null) {
                                    removedHIR.setState(Recency.HIR_NONRESIDENT);
                                    // We can't set the value to NULL_VALUE until we hold the
                                    // lock below
                                    queueContents[i] = removedHIR;
                                } else {
                                    removedHIR.setState(Recency.EVICTING);
                                    // It wasn't part of the stack so we have to remove it completely
                                    // Note we don't null out the queue or stack nodes on the LIRSNode
                                    // since we aren't reusing it
                                    queueContents[i] = removedHIR;
                                }
                                foundNode = true;
                                break;
                            case REMOVED:
                                // If it was removed then it wasn't evicted so we don't decrement
                                // size in that case
                                evictedValues--;
                                foundNode = true;
                                break;
                            case LIR_RESIDENT:
                            case EVICTING:
                            case EVICTED:
                            case HIR_NONRESIDENT:
                                break;
                        }
                    }
                }
            }
            BoundedEquivalentConcurrentHashMapV8.incrementSizeEviction(currentSize, -evictedValues, -decEvict);
            Collection<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> removedNodes = new ArrayList<>(queueContents.length);
            for (int j = 0; j < queueContents.length; ++j) {
                LIRSNode<K, V> evict = queueContents[j];
                // This can be null if the entry was removed when we tried to evict
                // or if we found a subsequent value in the same table
                if (evict == null) {
                    continue;
                }
                // If it is still evicting we lock the segment for the key and then
                // finally evict it
                // The locking of the outer segment is required if the entry is hit with
                // an update at the same time
                if (evict.state == Recency.EVICTING ||
                      evict.state == Recency.HIR_NONRESIDENT) {
                    // Most of the following is copied from putVal method of CHMV8
                    // This is so we can get the owning node so we can synchronize
                    // to make sure that no additional writes occur for the evicting
                    // value because we don't want them to be lost when we do the actual
                    // eviction
                    int hash = BoundedEquivalentConcurrentHashMapV8.spread(map.keyEq.hashCode(evict.getKey())); // EQUIVALENCE_MOD
                    for (BoundedEquivalentConcurrentHashMapV8.Node<K, V>[] tab = map.table; ; ) {
                        BoundedEquivalentConcurrentHashMapV8.Node<K, V> f;
                       int n, i;
                        if (tab == null || (n = tab.length) == 0 ||
                              (f = BoundedEquivalentConcurrentHashMapV8.tabAt(tab, i = (n - 1) & hash)) == null)
                            break;
                        else if (f.hash == BoundedEquivalentConcurrentHashMapV8.MOVED)
                            tab = map.helpTransfer(tab, f);
                        else {
                            synchronized (f) {
                                if (BoundedEquivalentConcurrentHashMapV8.tabAt(tab, i) == f) {
                                    synchronized (evict) {
                                        if (evict.state == Recency.EVICTING) {
                                            evict.setState(Recency.EVICTED);
                                            V prevValue = map.replaceNode(evict.getKey(), null, null, true);
                                            removedNodes.add(new BoundedEquivalentConcurrentHashMapV8.Node<>(-1, null, evict.getKey(),
                                                  prevValue, null));
                                        } else if (evict.state == Recency.HIR_NONRESIDENT) {
                                            BoundedEquivalentConcurrentHashMapV8.Node<K, V> node = f.find(hash, evict.getKey());
                                            V prevValue = node.val;
                                            if (prevValue != BoundedEquivalentConcurrentHashMapV8.NULL_VALUE) {
                                                node.val = (V) BoundedEquivalentConcurrentHashMapV8.NULL_VALUE;
                                                map.addCount(-1, -1);
                                                BoundedEquivalentConcurrentHashMapV8.Node<K, V> nonResidentNode = new BoundedEquivalentConcurrentHashMapV8.Node<K, V>(-1, null, evict.getKey(),
                                                      prevValue, null);
                                                removedNodes.add(nonResidentNode);
                                                map.notifyListenerOfRemoval(nonResidentNode, true);
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            return removedNodes;
        }
        return Collections.emptySet();
    }

    @Override
    public void onResize(long oldSize, long newSize) {
        // Do nothing
    }

    @Override
    public void resize(long newSize) {
        this.maximumSize = newSize;
        this.maximumHotSize = calculateLIRSize(this.maximumSize);
    }
}
