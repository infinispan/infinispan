package org.infinispan.container.offheap;

import java.util.Collections;
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
 * The link list consists of 28 bytes (3 longs and 1 int).  The first long is the actual entry address, the second is the
 * previous pointer, the third is the next pointer and lastly the int is the hashCode of the key to retrieve the lock.
 * @author wburns
 * @since 9.0
 */
public class BoundedOffHeapDataContainer extends OffHeapDataContainer {
   private final long maxSize;
   private final Lock lruLock;
   private final LongUnaryOperator sizeCalculator;

   private long currentSize;
   private long firstAddress;
   private long lastAddress;

   public BoundedOffHeapDataContainer(int desiredSize, long maxSize, EvictionType type) {
      super(desiredSize);
      this.maxSize = maxSize;
      if (type == EvictionType.COUNT) {
         sizeCalculator = i -> 1;
      } else {
         // Use size of entry plus 28 for our LRU pointer node
         sizeCalculator = i -> offHeapEntryFactory.determineSize(i) + 28;
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
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key, ComputeAction<WrappedBytes, WrappedBytes> action) {
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
         long lruNode = UNSAFE.getLong(oldAddress);
         if (trace) {
            log.tracef("Replacing LRU node: %d. OldValue: %d NewValue: %d", lruNode, oldAddress, newAddress);
         }
         // We have to update the lru node to point to the new address and vice versa
         UNSAFE.putLong(newAddress, lruNode);
         UNSAFE.putLong(lruNode, newAddress);

         moveToEnd(lruNode);

         currentSize += newSize;
         currentSize -= oldSize;
      } finally {
         lruLock.unlock();
      }
      super.entryReplaced(newAddress, oldAddress);
   }

   @Override
   protected void entryCreated(long newAddress) {
      int hashCode = offHeapEntryFactory.getHashCodeForAddress(newAddress);
      long newSize = sizeCalculator.applyAsLong(newAddress);
      lruLock.lock();
      try {
         currentSize += newSize;
         addEntryAddressToEnd(newAddress, hashCode);
      } finally {
         lruLock.unlock();
      }
      super.entryCreated(newAddress);
   }

   @Override
   protected void entryRemoved(long removedAddress) {
      long removedSize = sizeCalculator.applyAsLong(removedAddress);
      long lruNode = UNSAFE.getLong(removedAddress);
      lruLock.lock();
      try {
         // Current size has to be updated in the lock
         currentSize -=  removedSize;
         boolean middleNode = true;
         if (lruNode == lastAddress) {
            if (trace) {
               log.tracef("Removing last LRU node at %d", lruNode);
            }
            long previousLRUNode = UNSAFE.getLong(lruNode + 8);
            if (previousLRUNode != 0) {
               UNSAFE.putLong(previousLRUNode + 16, 0);
            }
            lastAddress = previousLRUNode;
            middleNode = false;
         }
         if (lruNode == firstAddress) {
            if (trace) {
               log.tracef("Removing first LRU node at %d", lruNode);
            }
            long nextLRUNode = UNSAFE.getLong(lruNode + 16);
            if (nextLRUNode != 0) {
               UNSAFE.putLong(nextLRUNode + 8, 0);
            }
            firstAddress = nextLRUNode;
            middleNode = false;
         }
         if (middleNode) {
            if (trace) {
               log.tracef("Removing middle LRU node at %d", lruNode);
            }
            // We are a middle pointer so both of these have to be non zero
            long previousLRUNode = UNSAFE.getLong(lruNode + 8);
            long nextLRUNode = UNSAFE.getLong(lruNode + 16);
            assert previousLRUNode != 0;
            assert nextLRUNode != 0;
            UNSAFE.putLong(previousLRUNode + 16, nextLRUNode);
            UNSAFE.putLong(nextLRUNode + 8, previousLRUNode);
         }
         allocator.deallocate(lruNode, 28);
      } finally {
         lruLock.unlock();
      }
      super.entryRemoved(removedAddress);
   }

   @Override
   protected void entryRetrieved(long entryAddress) {
      lruLock.lock();
      try {
         long lruNode = UNSAFE.getLong(entryAddress);
         if (trace) {
            log.tracef("Moving lruNode %d to the end which points at address %d", lruNode, entryAddress);
         }
         moveToEnd(lruNode);
      } finally {
         lruLock.unlock();
      }
      super.entryRetrieved(entryAddress);
   }

   @Override
   protected void performClear() {
      if (trace) {
         log.trace("Clearing bounded LRU entries");
      }
      // Technically we don't need to do lruLock since clear obtains all write locks first
      lruLock.lock();
      try {
         long address = firstAddress;
         while (address != 0) {
            long nextAddress = UNSAFE.getLong(address + 16);
            allocator.deallocate(address, 28);
            address = nextAddress;
         }
         currentSize = 0;
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

   private void ensureSize() {
      while (true) {
         long addressToRemove;
         Lock entryWriteLock;
         lruLock.lock();
         try {
            if (currentSize > maxSize) {
               // Retrieve the hashCode so we can lock it to verify the address is still present
               int hashCode = UNSAFE.getInt(firstAddress + 24);
               entryWriteLock = locks.getLockFromHashCode(hashCode).writeLock();
               if (!entryWriteLock.tryLock()) {
                  // Attempts to release the lru lock and reacquire the write lock are problematic as underlying allocator
                  // may return same memory address between points, so there is no way to verify we have same object
                  // in an efficient way - just force loop back around to allow other threads to get lruLock
                  addressToRemove = 0;
               } else {
                  addressToRemove = UNSAFE.getLong(firstAddress);
               }
            } else {
               // This is the only way to break out of loop
               break;
            }
         } finally {
            lruLock.unlock();
         }

         // We can proceed with removal if it is non zero.  It would be 0 if the firstAddress changed
         // It is assumed we own the entryWriteLock lock as well so we can read the keys.
         if (addressToRemove != 0) {
            if (trace) {
               log.tracef("Removing entry: %d due to eviction due to size %d being larger than maximum of %d",
                     addressToRemove, currentSize, maxSize);
            }
            try {
               InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(addressToRemove);
               performRemove(addressToRemove, offHeapEntryFactory.getKey(addressToRemove));
               evictionManager.onEntryEviction(Collections.singletonMap(ice.getKey(), ice));
            } finally {
               entryWriteLock.unlock();
            }
         } else {
            // Just to let another thread possibly continue and release its lock
            Thread.yield();
         }
      }
   }

   /**
    * Method to be invoked when adding a new entry address to the end of the lru nodes.  This occurs for newly created
    * entries.
    * This method should only be invoked after acquiring the lruLock
    * @param entryAddress the new entry address pointer *NOT* the lru node
    */
   private void addEntryAddressToEnd(long entryAddress, int hashCode) {
      long nodeAddress = allocator.allocate(28);
      if (trace) {
         log.tracef("Creating LRU node %d for new entry %d", nodeAddress, entryAddress);
      }
      // First update the pointer to our new entry address
      UNSAFE.putLong(nodeAddress, entryAddress);
      // Also our entry address needs a pointer to its lru node
      UNSAFE.putLong(entryAddress, nodeAddress);
      // This means it is the first entry
      if (lastAddress == 0) {
         firstAddress = nodeAddress;
         lastAddress = nodeAddress;
         // Have to make sure the memory is cleared so we don't use unitialized values
         UNSAFE.putLong(nodeAddress + 8, 0);
      } else {
         // Writes back pointer to the old lastAddress
         UNSAFE.putLong(nodeAddress + 8, lastAddress);
         // Write the forward pointer in old lastAddress to point to us
         UNSAFE.putLong(lastAddress + 16, nodeAddress);
         // Finally make us the last address
         lastAddress = nodeAddress;
      }
      // Since we are last there is no pointer after us
      UNSAFE.putLong(nodeAddress + 16, 0);
      UNSAFE.putInt(nodeAddress + 24, hashCode);
   }

   /**
    * Method to be invoked when moving an existing lru node to the end.  This occurs when the entry is accessed for this
    * node.
    * This method should only be invoked after acquiring the lruLock.
    * @param lruNode the node to move to the end
    */
   private void moveToEnd(long lruNode) {
      if (lruNode != lastAddress) {
         long nextLruNode = UNSAFE.getLong(lruNode + 16);
         if (lruNode == firstAddress) {
            UNSAFE.putLong(nextLruNode + 8, 0);
            firstAddress = nextLruNode;
         } else {
            long prevLruNode = UNSAFE.getLong(lruNode + 8);
            UNSAFE.putLong(prevLruNode + 16, nextLruNode);
            UNSAFE.putLong(nextLruNode + 8, prevLruNode);
         }
         // Link the previous last node to our new last node
         UNSAFE.putLong(lastAddress + 16, lruNode);
         // Sets the previous node of our new tail node to the previous tail node
         UNSAFE.putLong(lruNode + 8, lastAddress);
         UNSAFE.putLong(lruNode + 16, 0);
         lastAddress = lruNode;
      }
   }
}
