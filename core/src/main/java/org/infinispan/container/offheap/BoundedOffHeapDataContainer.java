package org.infinispan.container.offheap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionType;
import org.infinispan.metadata.Metadata;

/**
 * Data Container implementation that stores entries in native memory (off-heap) that is also bounded.  This
 * implementation uses a simple LRU doubly linked list off-heap guarded by a single lock.
 * <p>
 * The link list is represented by firstAddress as the had of the list and lastAddress as the tail of the list. Each
 * entry in the list consists of 28 bytes (3 longs and 1 int), the first long is the actual entry address, the second is
 * a pointer to the previous element in the list, the third is the next pointer and lastly the int is the hashCode of
 * the key to retrieve the lock. The hashCode is required to know which lock to use when trying to read the entry.
 *
 * @author wburns
 * @since 9.0
 */
public class BoundedOffHeapDataContainer extends OffHeapDataContainer {
   protected final long maxSize;
   protected final Lock lruLock;
   protected final LongUnaryOperator sizeCalculator;
   protected final long initialSize;

   protected long currentSize;
   protected long firstAddress;
   protected long lastAddress;

   public BoundedOffHeapDataContainer(int desiredSize, long maxSize, EvictionType type) {
      super(desiredSize);
      this.maxSize = maxSize;
      if (type == EvictionType.COUNT) {
         sizeCalculator = i -> 1;
         initialSize = 0;
      } else {
         // Use size of entry plus 16 for our LRU pointers
         sizeCalculator = i -> offHeapEntryFactory.getSize(i);
         // We have to make sure to count the address hash as part of our size
         initialSize = UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(memoryAddressCount << 3);
         currentSize = initialSize;
      }
      this.lruLock = new ReentrantLock();
      firstAddress = 0;
   }

   @Override
   public void put(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      super.put(key, value, metadata);
      // The following is called outside of the write lock specifically - since we may not have to evict and even
      // if we did it would quite possibly need a different lock
      ensureSize();
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key,
                                                                 ComputeAction<WrappedBytes, WrappedBytes> action) {
      InternalCacheEntry<WrappedBytes, WrappedBytes> result = super.compute(key, action);
      if (result != null) {
         // Means we had a put or replace called so we have to confirm sizes
         // The following is called outside of the write lock specifically - since we may not have to evict and even
         // if we did it would quite possibly need a different lock
         ensureSize();
      }
      return result;
   }

   @Override
   protected void entryReplaced(long newAddress, long oldAddress) {
      long oldSize = sizeCalculator.applyAsLong(oldAddress);
      long newSize = sizeCalculator.applyAsLong(newAddress);
      lruLock.lock();
      try {
         removeNode(oldAddress);
         addEntryAddressToEnd(newAddress);

         currentSize += newSize;
         currentSize -= oldSize;
         // Must only remove entry while holding lru lock now
         super.entryReplaced(newAddress, oldAddress);
      } finally {
         lruLock.unlock();
      }
   }

   @Override
   protected void entryCreated(long newAddress) {
      long newSize = sizeCalculator.applyAsLong(newAddress);
      lruLock.lock();
      try {
         currentSize += newSize;
         addEntryAddressToEnd(newAddress);
         super.entryCreated(newAddress);
      } finally {
         lruLock.unlock();
      }
   }

   @Override
   protected void entryRemoved(long removedAddress) {
      long removedSize = sizeCalculator.applyAsLong(removedAddress);
      lruLock.lock();
      try {
         // Current size has to be updated in the lock
         currentSize -=  removedSize;
         removeNode(removedAddress);
         // Removals are only done while holding lru lock now
         super.entryRemoved(removedAddress);
      } finally {
         lruLock.unlock();
      }
   }

   /**
    * Removes the address node and updates previous and next lru node pointers properly
    * The {@link BoundedOffHeapDataContainer#lruLock} <b>must</b> be held when invoking this
    * @param address
    */
   private void removeNode(long address) {
      boolean middleNode = true;
      if (address == lastAddress) {
         if (trace) {
            log.tracef("Removed entry 0x%016x from the end of the LRU list", address);
         }
         long previousLRUNode = OffHeapLruNode.getPrevious(address);
         if (previousLRUNode != 0) {
            OffHeapLruNode.setNext(previousLRUNode, 0);
         }
         lastAddress = previousLRUNode;
         middleNode = false;
      }
      if (address == firstAddress) {
         if (trace) {
            log.tracef("Removed entry 0x%016x from the beginning of the LRU list", address);
         }
         long nextLRUNode = OffHeapLruNode.getNext(address);
         if (nextLRUNode != 0) {
            OffHeapLruNode.setPrevious(nextLRUNode, 0);
         }
         firstAddress = nextLRUNode;
         middleNode = false;
      }
      if (middleNode) {
         if (trace) {
            log.tracef("Removed entry 0x%016x from the middle of the LRU list", address);
         }
         // We are a middle pointer so both of these have to be non zero
         long previousLRUNode = OffHeapLruNode.getPrevious(address);
         long nextLRUNode = OffHeapLruNode.getNext(address);
         assert previousLRUNode != 0;
         assert nextLRUNode != 0;
         OffHeapLruNode.setNext(previousLRUNode, nextLRUNode);
         OffHeapLruNode.setPrevious(nextLRUNode, previousLRUNode);
      }
   }

   @Override
   protected void entryRetrieved(long entryAddress) {
      lruLock.lock();
      try {
         if (trace) {
            log.tracef("Moving entry 0x%016x to the end of the LRU list", entryAddress);
         }
         moveToEnd(entryAddress);
         super.entryRetrieved(entryAddress);
      } finally {
         lruLock.unlock();
      }
   }

   @Override
   protected void performClear() {
      if (trace) {
         log.trace("Clearing bounded LRU entries");
      }
      // Technically we don't need to do lruLock since clear obtains all write locks first
      lruLock.lock();
      try {
         currentSize = initialSize;
         firstAddress = 0;
         lastAddress = 0;
      } finally {
         lruLock.unlock();
      }
      if (trace) {
         log.trace("Cleared bounded LRU entries");
      }
      super.performClear();
   }

   /**
    * This method repeatedly removes the head of the LRU list until there the current size is less than or equal to
    * `maxSize`.
    * <p>
    * We need to hold the LRU lock in order to check the current size and to read the head entry,
    * and then we need to hold the head entry's write lock in order to remove it.
    * The problem is that the correct acquisition order is entry write lock first, LRU lock second,
    * and we need to hold the LRU lock so that we know which entry write lock to acquire.
    * <p>
    * To work around it, we first try to acquire the entry write lock without blocking.
    * If that fails, we release the LRU lock and we acquire the locks in the correct order, hoping that
    * the LRU head doesn't change while we wait. Because the entry write locks are striped, we actually
    * tolerate a LRU head change as long as the new head entry is in the same lock stripe.
    * If the LRU list head changes, we release both locks and try again.
    */
   private void ensureSize() {

      while (true) {
         long addressToRemove;
         Lock entryWriteLock;
         lruLock.lock();
         try {
            if (currentSize <= maxSize) {
               break;
            }
            int hashCode = offHeapEntryFactory.getHashCode(firstAddress);
            entryWriteLock = locks.getLockFromHashCode(hashCode).writeLock();
            if (!entryWriteLock.tryLock()) {
               addressToRemove = 0;
            } else {
               addressToRemove = firstAddress;
            }
         } finally {
            lruLock.unlock();
         }

         if (addressToRemove == 0) {
            entryWriteLock.lock();
            try {
               lruLock.lock();
               try {
                  if (currentSize <= maxSize) {
                     break;
                  }
                  int hashCode = offHeapEntryFactory.getHashCode(firstAddress);
                  Lock innerLock = locks.getLockFromHashCode(hashCode).writeLock();
                  if (innerLock == entryWriteLock) {
                     addressToRemove = firstAddress;
                  } else {
                     addressToRemove = 0;
                  }
               } finally {
                  lruLock.unlock();
               }
            } finally {
               if (addressToRemove == 0) {
                  entryWriteLock.unlock();
               }
            }
         }

         if (addressToRemove != 0) {
            if (trace) {
               log.tracef("Removing entry: 0x%016x due to eviction due to size %d being larger than maximum of %d",
                          addressToRemove, currentSize, maxSize);
            }
            try {
               InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(addressToRemove);
               passivator.passivate(ice);
               performRemove(memoryLookup.getMemoryAddress(ice.getKey()), addressToRemove, ice.getKey(), false);
               evictionManager.onEntryEviction(Collections.singletonMap(ice.getKey(), ice));
            } finally {
               entryWriteLock.unlock();
            }
         }
      }
   }

   /**
    * Method to be invoked when adding a new entry address to the end of the lru nodes.  This occurs for newly created
    * entries.
    * This method should only be invoked after acquiring the lruLock
    *
    * @param entryAddress the new entry address pointer *NOT* the lru node
    */
   private void addEntryAddressToEnd(long entryAddress) {
      if (trace) {
         log.tracef("Adding entry 0x%016x to the end of the LRU list", entryAddress);
      }
      // This means it is the first entry
      if (lastAddress == 0) {
         firstAddress = entryAddress;
         lastAddress = entryAddress;
         // Have to make sure the memory is cleared so we don't use unitialized values
         OffHeapLruNode.setPrevious(entryAddress, 0);
      } else {
         // Writes back pointer to the old lastAddress
         OffHeapLruNode.setPrevious(entryAddress, lastAddress);
         // Write the forward pointer in old lastAddress to point to us
         OffHeapLruNode.setNext(lastAddress, entryAddress);
         // Finally make us the last address
         lastAddress = entryAddress;
      }
      // Since we are last there is no pointer after us
      OffHeapLruNode.setNext(entryAddress, 0);
   }

   /**
    * Method to be invoked when moving an existing lru node to the end.  This occurs when the entry is accessed for this
    * node.
    * This method should only be invoked after acquiring the lruLock.
    *
    * @param lruNode the node to move to the end
    */
   private void moveToEnd(long lruNode) {
      if (lruNode != lastAddress) {
         long nextLruNode = OffHeapLruNode.getNext(lruNode);
         assert nextLruNode != 0;
         if (lruNode == firstAddress) {
            OffHeapLruNode.setPrevious(nextLruNode, 0);
            firstAddress = nextLruNode;
         } else {
            long prevLruNode = OffHeapLruNode.getPrevious(lruNode);
            assert prevLruNode != 0;
            OffHeapLruNode.setNext(prevLruNode, nextLruNode);
            OffHeapLruNode.setPrevious(nextLruNode, prevLruNode);
         }
         // Link the previous last node to our new last node
         OffHeapLruNode.setNext(lastAddress, lruNode);
         // Sets the previous node of our new tail node to the previous tail node
         OffHeapLruNode.setPrevious(lruNode, lastAddress);
         OffHeapLruNode.setNext(lruNode, 0);
         lastAddress = lruNode;
      }
   }

   @SuppressWarnings("unused")
   private List<String> debugLruList() {
      if (firstAddress == 0)
         return Collections.emptyList();

      lruLock.lock();
      try {
         List<String> list = new ArrayList<>(sizeIncludingExpired());
         for (long a = firstAddress; a != 0; a = OffHeapLruNode.getNext(a)) {
            long n = OffHeapLruNode.getNext(a);
            list.add(OffHeapLruNode.debugString(a));
            assert n == 0 || OffHeapLruNode.getPrevious(n) == a;
         }
         return list;
      } finally {
         lruLock.unlock();
      }
   }
}
