package org.infinispan.container.offheap;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractDelegatingInternalDataContainer;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionType;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author wburns
 * @since 9.4
 */
public class SegmentedBoundedOffHeapDataContainer extends AbstractDelegatingInternalDataContainer<WrappedBytes, WrappedBytes> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final OffHeapMapSupplier offHeapMapSupplier;
   private final OffHeapListener offHeapListener;

   @Inject ComponentRegistry componentRegistry;

   @Inject protected OffHeapMemoryAllocator allocator;
   @Inject protected OffHeapEntryFactory offHeapEntryFactory;

   @Inject protected EvictionManager evictionManager;
   @Inject protected PassivationManager passivator;

   protected final long maxSize;
   protected final Lock lruLock;
   protected final LongUnaryOperator sizeCalculator;

   protected long currentSize;
   protected long firstAddress;
   protected long lastAddress;

   protected DefaultSegmentedDataContainer dataContainer;

   public SegmentedBoundedOffHeapDataContainer(int addressCount, int numSegments, long maxSize, EvictionType type) {
      int sizePerSegment = addressCount / numSegments;
      offHeapListener = new OffHeapListener();

      this.maxSize = maxSize;
      if (type == EvictionType.COUNT) {
         offHeapMapSupplier = new OffHeapMapSupplier(false, sizePerSegment);
         sizeCalculator = i -> 1;
      } else {
         offHeapMapSupplier = new OffHeapMapSupplier(true, sizePerSegment);
         // Use size of entry plus 16 for our LRU pointers
         sizeCalculator = i -> offHeapEntryFactory.getSize(i);
      }
      this.lruLock = new ReentrantLock();
      firstAddress = 0;

      dataContainer = new DefaultSegmentedDataContainer<>(offHeapMapSupplier, numSegments);
   }

   @Start
   public void start() {
      componentRegistry.wireDependencies(dataContainer);
      dataContainer.start();
   }

   @Stop(priority = 999)
   public void stop() {
      dataContainer.stop();
   }

   @Override
   protected InternalDataContainer<WrappedBytes, WrappedBytes> delegate() {
      return dataContainer;
   }


   @Override
   public void put(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      super.put(key, value, metadata);
      // The following is called outside of the write lock specifically - since we may not have to evict and even
      // if we did it would quite possibly need a different lock
      ensureSize();
   }

   @Override
   public void put(int segment, WrappedBytes wrappedBytes, WrappedBytes wrappedBytes2, Metadata metadata) {
      super.put(segment, wrappedBytes, wrappedBytes2, metadata);
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
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(int segment, WrappedBytes key, ComputeAction<WrappedBytes, WrappedBytes> action) {
      InternalCacheEntry<WrappedBytes, WrappedBytes> result = super.compute(segment, key, action);
      if (result != null) {
         // Means we had a put or replace called so we have to confirm sizes
         // The following is called outside of the write lock specifically - since we may not have to evict and even
         // if we did it would quite possibly need a different lock
         ensureSize();
      }
      return result;
   }

   protected OffHeapConcurrentMap getMapThatContainsKey(byte[] key) {
      int segment = dataContainer.getSegmentForKey(key);

      // This can become null if we have a concurrent removal of segments
      return (OffHeapConcurrentMap) dataContainer.getMapForSegment(segment);
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
         OffHeapConcurrentMap map;
         lruLock.lock();
         try {
            if (currentSize <= maxSize) {
               break;
            }
            // We shouldn't be able to get into this state
            assert firstAddress > 0;
            // We read the key before hashCode due to how off heap bytes are written (key requires reading metadata
            // which comes before hashCode, which should keep hashCode bytes in memory register in most cases)
            byte[] key = offHeapEntryFactory.getKey(firstAddress);

            map = getMapThatContainsKey(key);
            if (map != null) {
               int hashCode = offHeapEntryFactory.getHashCode(firstAddress);
               // This is always non null
               entryWriteLock = map.getLocks().getLockFromHashCode(hashCode).writeLock();
               if (entryWriteLock.tryLock()) {
                  addressToRemove = firstAddress;
               } else {
                  addressToRemove = 0;
               }
            } else {
               // We have to loop back around - more than likely the concurrent removal of a segment probably reduced
               // the max size so we probably don't have to worry
               continue;
            }
         } finally {
            lruLock.unlock();
         }

         // If we got here it means we were unable to acquire the write lock, so we have to attempt a blocking
         // write lock and then acquire the lruLock, since they have to be acquired in that order (exception using
         // try lock as above)
         if (addressToRemove == 0) {
            entryWriteLock.lock();
            try {
               lruLock.lock();
               try {
                  if (currentSize <= maxSize) {
                     break;
                  }
                  // Now that we have locks we have to verify the first address is protected by the same lock still
                  byte[] key = offHeapEntryFactory.getKey(firstAddress);

                  OffHeapConcurrentMap protectedMap = getMapThatContainsKey(key);
                  if (protectedMap == map) {
                     int hashCode = offHeapEntryFactory.getHashCode(firstAddress);
                     Lock innerLock = map.getLocks().getLockFromHashCode(hashCode).writeLock();
                     if (innerLock == entryWriteLock) {
                        addressToRemove = firstAddress;
                     }
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
               map.remove(ice.getKey(), addressToRemove);
               evictionManager.onEntryEviction(Collections.singletonMap(ice.getKey(), ice));
            } finally {
               entryWriteLock.unlock();
            }
         }
      }
   }

   private class OffHeapMapSupplier implements Supplier<ConcurrentMap<WrappedBytes,
         InternalCacheEntry<WrappedBytes, WrappedBytes>>> {
      private final boolean addAllocationSize;
      private final int allocationSize;

      /**
       * Supplier that creates a new OffHeapConcurrentMap per invocation
       * @param addAllocationSize whether starting/stopping map should add the alloction size to count
       * @param allocationSize rounded up to nearest power of two
       */
      private OffHeapMapSupplier(boolean addAllocationSize, int allocationSize) {
         this.addAllocationSize = addAllocationSize;
         this.allocationSize = Util.findNextHighestPowerOfTwo(allocationSize);
      }

      @Override
      public ConcurrentMap<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>> get() {
         // OffHeap concurrent map will round allocationSize to nearest power of 2
         OffHeapConcurrentMap map = new OffHeapConcurrentMap(allocationSize, allocator, offHeapEntryFactory,
               offHeapListener) {
            @Override
            public void stop() {
               super.stop();
               if (addAllocationSize) {
                  lruLock.lock();
                  try {
                     currentSize -= UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(allocationSize << 3);
                  } finally {
                     lruLock.unlock();
                  }
               }
            }
         };
         if (addAllocationSize) {
            lruLock.lock();
            try {
               currentSize += UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(allocationSize << 3);
            } finally {
               lruLock.unlock();
            }
         }
         map.start();
         return map;
      }
   }

   private class OffHeapListener implements OffHeapConcurrentMap.EntryListener {

      @Override
      public void entryCreated(long newAddress) {
         long newSize = sizeCalculator.applyAsLong(newAddress);
         lruLock.lock();
         try {
            currentSize += newSize;
            addEntryAddressToEnd(newAddress);
         } finally {
            lruLock.unlock();
         }
      }

      @Override
      public void entryRemoved(long removedAddress) {
         long removedSize = sizeCalculator.applyAsLong(removedAddress);
         lruLock.lock();
         try {
            // Current size has to be updated in the lock
            currentSize -=  removedSize;
            removeNode(removedAddress);
            allocator.deallocate(removedAddress);
         } finally {
            lruLock.unlock();
         }
      }

      @Override
      public void entryReplaced(long newAddress, long oldAddress) {
         long oldSize = sizeCalculator.applyAsLong(oldAddress);
         long newSize = sizeCalculator.applyAsLong(newAddress);
         lruLock.lock();
         try {
            removeNode(oldAddress);
            addEntryAddressToEnd(newAddress);

            currentSize += newSize;
            currentSize -= oldSize;
            // Must only remove entry while holding lru lock now
            allocator.deallocate(oldAddress);
         } finally {
            lruLock.unlock();
         }
      }

      @Override
      public void entryRetrieved(long entryAddress) {
         lruLock.lock();
         try {
            if (trace) {
               log.tracef("Moving entry 0x%016x to the end of the LRU list", entryAddress);
            }
            moveToEnd(entryAddress);
         } finally {
            lruLock.unlock();
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
   }

   @Override
   public long capacity() {
      return maxSize;
   }

   @Override
   public long evictionSize() {
      return currentSize;
   }
}
