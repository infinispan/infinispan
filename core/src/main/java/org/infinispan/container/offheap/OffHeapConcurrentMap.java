package org.infinispan.container.offheap;

import java.lang.invoke.MethodHandles;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.PeekableMap;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author wburns
 * @since 9.4
 */
public class OffHeapConcurrentMap implements ConcurrentMap<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>,
      PeekableMap<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>, Lifecycle {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   // Max would be 1:1 ratio with memory addresses - must be a crazy machine to have that many processors
   private final static int MAX_ADDRESS_COUNT = 1 << 30;

   private final AtomicLong size = new AtomicLong();
   private final int lockCount;
   private final int memoryAddressCount;
   private final StripedLock locks;

   private final OffHeapMemoryAllocator allocator;
   private final OffHeapEntryFactory offHeapEntryFactory;
   private final OffsetCalculator offsetCalculator;

   private final EntryListener listener;

   // Objects modified from start/stop
   private MemoryAddressHash memoryLookup;

   // Variable to make sure memory locations aren't read after being deallocated
   // This variable should always be read first after acquiring either the read or write lock
   private boolean dellocated = false;

   /**
    * Listener interface that is notified when certain operations occur for various memory addresses. Note that when
    * this listener is used certain operations are not performed and require the listener to do these instead. Please
    * note each method documentation to tell what those are.
    */
   public interface EntryListener {
      /**
       * Invoked when an entry is about to be created.  The new address is fully addressable,
       * The write lock will already be acquired for the given segment the key mapped to.
       * @param newAddress the address just created that will be the new entry
       */
      void entryCreated(long newAddress);

      /**
       * Invoked when an entry is about to be removed.  You can read values from this but after this method is completed
       * this memory address may be freed. The write lock will already be acquired for the given segment the key mapped to.
       * <p>
       * This method <b>MUST</b> free the removedAddress before returning
       * @param removedAddress the address about to be removed
       */
      void entryRemoved(long removedAddress);

      /**
       * Invoked when an entry is about to be replaced with a new one.  The old and new address are both addressable,
       * however oldAddress may be freed after this method returns.  The write lock will already be acquired for the given
       * segment the key mapped to.
       * <p>
       * This method <b>MUST</b> free the oldAddress before returning
       * @param newAddress the address just created that will be the new entry
       * @param oldAddress the old address for this entry that will be soon removed
       */
      void entryReplaced(long newAddress, long oldAddress);

      /**
       * Invoked when an entry is successfully retrieved.  The read lock will already
       * be acquired for the given segment the key mapped to.
       * @param entryAddress the address of the entry retrieved
       */
      void entryRetrieved(long entryAddress);
   }

   private void entryCreated(long newAddress) {
      if (listener != null) {
         listener.entryCreated(newAddress);
      }
   }

   private void entryRemoved(long removedAddress) {
      if (listener != null) {
         listener.entryRemoved(removedAddress);
      } else {
         allocator.deallocate(removedAddress);
      }
   }

   private void entryReplaced(long newAddress, long oldAddress) {
      if (listener != null) {
         listener.entryReplaced(newAddress, oldAddress);
      } else {
         allocator.deallocate(oldAddress);
      }
   }

   private void entryRetrieved(long entryAddress) {
      if (listener != null) {
         listener.entryRetrieved(entryAddress);
      }
   }

   public OffHeapConcurrentMap(int desiredSize, OffHeapMemoryAllocator allocator,
         OffHeapEntryFactory offHeapEntryFactory, EntryListener listener) {
      this.allocator = allocator;
      this.offHeapEntryFactory = offHeapEntryFactory;
      this.listener = listener;

      // Since these are segmented now, just use # of processors instead
      lockCount = Util.findNextHighestPowerOfTwo(ProcessorInfo.availableProcessors() << 1);
      memoryAddressCount = getActualAddressCount(desiredSize, lockCount);
      offsetCalculator = new ContiguousOffsetCalculator(lockCount);
      // Unfortunately desired size directly correlates to lock size
      locks = new StripedLock(lockCount, offsetCalculator);
   }

   private void checkDeallocation() {
      if (dellocated) {
         throw new IllegalStateException("Map was already shut down!");
      }
   }

   static int getActualAddressCount(int desiredSize, int lockCount) {
      int memoryAddresses = desiredSize >= MAX_ADDRESS_COUNT ? MAX_ADDRESS_COUNT : lockCount;
      while (memoryAddresses < desiredSize) {
         memoryAddresses <<= 1;
      }
      return memoryAddresses;
   }

   public StripedLock getLocks() {
      return locks;
   }

   @Override
   public void start() {
      locks.lockAll();
      try {
         memoryLookup = new MemoryAddressHash(memoryAddressCount, offsetCalculator, allocator);
         dellocated = false;
      } finally {
         locks.unlockAll();
      }
   }

   @Override
   public void stop() {
      locks.lockAll();
      try {
         clear();
         memoryLookup.deallocate();
         dellocated = true;
      } finally {
         locks.unlockAll();
      }
   }

   @Override
   public int size() {
      return (int) Math.min(size.get(), Integer.MAX_VALUE);
   }

   @Override
   public boolean isEmpty() {
      return size.get() == 0;
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key, BiFunction<? super WrappedBytes,
         ? super InternalCacheEntry<WrappedBytes, WrappedBytes>, ? extends InternalCacheEntry<WrappedBytes, WrappedBytes>> remappingFunction) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long bucketAddress = memoryLookup.getMemoryAddress(key);
         long actualAddress = bucketAddress == 0 ? 0 : performGet(bucketAddress, key);
         InternalCacheEntry<WrappedBytes, WrappedBytes> prev;
         if (actualAddress != 0) {
            prev = offHeapEntryFactory.fromMemory(actualAddress);
         } else {
            prev = null;
         }
         InternalCacheEntry<WrappedBytes, WrappedBytes> result = remappingFunction.apply(key, prev);
         if (prev == result) {
            // noop
         } else if (result != null) {
            long newAddress = offHeapEntryFactory.create(key, result.getValue(), result.getMetadata());
            // TODO: Technically actualAddress could be a 0 and bucketAddress != 0, which means we will loop through
            // entire bucket for no reason as it will never match (doing key equality checks)
            performPut(bucketAddress, actualAddress, newAddress, key, false);
         } else {
            // result is null here - so we remove the entry
            performRemove(bucketAddress, actualAddress, key, null, false);
         }
         return result;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean containsKey(Object key) {
      if (!(key instanceof WrappedBytes)) {
         return false;
      }
      Lock lock = locks.getLock(key).readLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         if (address == 0) {
            return false;
         }

         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            if (offHeapEntryFactory.equalsKey(address, (WrappedBytes) key)) {
               return !offHeapEntryFactory.isExpired(address);
            }
            address = nextAddress;
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean containsValue(Object value) {
      return false;
   }

   private InternalCacheEntry<WrappedBytes, WrappedBytes> peekOrGet(WrappedBytes k, boolean peek) {
      Lock lock = locks.getLock(k).readLock();
      lock.lock();
      try {
         checkDeallocation();
         long bucketAddress = memoryLookup.getMemoryAddress(k);
         if (bucketAddress == 0) {
            return null;
         }

         long actualAddress = performGet(bucketAddress, k);
         if (actualAddress != 0) {
            InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(actualAddress);
            if (!peek) {
               entryRetrieved(actualAddress);
            }
            return ice;
         }
      } finally {
         lock.unlock();
      }
      return null;
   }

   /**
    * Gets the actual address for the given key in the given bucket or 0 if it isn't present or expired
    * @param bucketHeadAddress the starting address of the address hash
    * @param k the key to retrieve the address for it if matches
    * @return the address matching the key or 0
    */
   private long performGet(long bucketHeadAddress, WrappedBytes k) {
      long address = bucketHeadAddress;
      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         if (offHeapEntryFactory.equalsKey(address, k)) {
            break;
         } else {
            address = nextAddress;
         }
      }
      return address;
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> get(Object key) {
      if (!(key instanceof WrappedBytes)) {
         return null;
      }
      return peekOrGet((WrappedBytes) key, false);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> peek(Object key) {
      if (!(key instanceof WrappedBytes)) {
         return null;
      }
      return peekOrGet((WrappedBytes) key, true);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> put(WrappedBytes key,
         InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long newAddress = offHeapEntryFactory.create(key, value.getValue(), value.getMetadata());
         long address = memoryLookup.getMemoryAddress(key);
         return performPut(address, 0, newAddress, key, true);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Performs the actual put operation putting the new address into the memory lookups.  The write lock for the given
    * key <b>must</b> be held before calling this method.
    * @param bucketHeadAddress the entry address of the first element in the lookup
    * @param actualAddress the actual address if it is known or 0. By passing this != 0 equality checks can be bypassed.
    *                      If a value of 0 is provided this will use key equality.
    * @param newAddress the address of the new entry
    * @param key the key of the entry
    * @param requireReturn whether the return value is required
    * @return {@code true} if the entry doesn't exists in memory and was newly create, {@code false} otherwise
    */
   private InternalCacheEntry<WrappedBytes, WrappedBytes> performPut(long bucketHeadAddress, long actualAddress,
         long newAddress, WrappedBytes key, boolean requireReturn) {
      // Have to start new linked node list
      if (bucketHeadAddress == 0) {
         memoryLookup.putMemoryAddress(key, newAddress);
         entryCreated(newAddress);
         size.incrementAndGet();
         return null;
      } else {
         boolean replaceHead = false;
         boolean foundPrevious = false;
         // Whether the key was found or not - short circuit equality checks
         InternalCacheEntry<WrappedBytes, WrappedBytes> previousValue = null;
         long address = bucketHeadAddress;
         // Holds the previous linked list address
         long prevAddress = 0;
         // Keep looping until we get the tail end - we always append the put to the end
         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            if (!foundPrevious) {
               // If the actualAddress was not known check key equality otherwise just compare with the address
               if (actualAddress == 0 ? offHeapEntryFactory.equalsKey(address, key) : actualAddress == address) {
                  foundPrevious = true;
                  if (requireReturn) {
                     previousValue = offHeapEntryFactory.fromMemory(address);
                  }
                  entryReplaced(newAddress, address);
                  // If this is true it means this was the first node in the linked list
                  if (prevAddress == 0) {
                     if (nextAddress == 0) {
                        // This branch is the case where our key is the only one in the linked list
                        replaceHead = true;
                     } else {
                        // This branch is the case where our key is the first with another after
                        memoryLookup.putMemoryAddress(key, nextAddress);
                     }
                  } else {
                     // This branch means our node was not the first, so we have to update the address before ours
                     // to the one we previously referenced
                     offHeapEntryFactory.setNext(prevAddress, nextAddress);
                     // We purposely don't update prevAddress, because we have to keep it as the current pointer
                     // since we removed ours
                     address = nextAddress;
                     continue;
                  }
               }
            }
            prevAddress = address;
            address = nextAddress;
         }
         // If we didn't find the key previous, it means we are a new entry
         if (!foundPrevious) {
            entryCreated(newAddress);
            size.incrementAndGet();
         }
         if (replaceHead) {
            memoryLookup.putMemoryAddress(key, newAddress);
         } else {
            // Now prevAddress should be the last link so we fix our link
            offHeapEntryFactory.setNext(prevAddress, newAddress);
         }
         return previousValue;
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> remove(Object key) {
      if (!(key instanceof WrappedBytes)) {
         return null;
      }
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         if (address == 0) {
            return null;
         }
         return performRemove(address, 0, (WrappedBytes) key, null, true);
      } finally {
         lock.unlock();
      }
   }

   /**
    * This method assumes that the write lock has already been acquired via {@link #getLocks()} and getting the write
    * lock for this key.
    * <p>
    * This method will avoid some additional lookups ass the memory address is already acquired and not return
    * the old entry.
    * @param key key to remove
    * @param address the address for the key
    */
   void remove(WrappedBytes key, long address) {
      long bucketAddress = memoryLookup.getMemoryAddress(key);
      assert bucketAddress != 0;
      performRemove(bucketAddress, address, key, null, false);
   }

   /**
    * Performs the actual remove operation removing the new address from the memory lookups.  The write lock for the given
    * key <b>must</b> be held before calling this method.
    * @param bucketHeadAddress the starting address of the address hash
    * @param actualAddress the actual address if it is known or 0. By passing this != 0 equality checks can be bypassed.
    *                      If a value of 0 is provided this will use key equality. key is not required when this != 0
    * @param key the key of the entry
    * @param value the value to match if present
    * @param requireReturn whether this method is forced to return the entry removed (optimizations can be done if
    *                      the entry is not needed)
    */
   private InternalCacheEntry<WrappedBytes, WrappedBytes> performRemove(long bucketHeadAddress, long actualAddress,
         WrappedBytes key, WrappedBytes value, boolean requireReturn) {
      long prevAddress = 0;
      // We only use the head pointer for the first iteration
      long address = bucketHeadAddress;
      InternalCacheEntry<WrappedBytes, WrappedBytes> ice = null;

      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         boolean removeThisAddress;
         // If the actualAddress was not known, check key equality otherwise just compare with the address
         removeThisAddress = actualAddress == 0 ? offHeapEntryFactory.equalsKey(address, key) : actualAddress == address;
         if (removeThisAddress) {
            if (value != null) {
               ice = offHeapEntryFactory.fromMemory(address);
               // If value doesn't match and was provided then don't remove it
               if (!value.equalsWrappedBytes(ice.getValue())) {
                  ice = null;
                  break;
               }
            }
            if (requireReturn && ice == null) {
               ice = offHeapEntryFactory.fromMemory(address);
            }
            entryRemoved(address);
            if (prevAddress != 0) {
               offHeapEntryFactory.setNext(prevAddress, nextAddress);
            } else {
               memoryLookup.putMemoryAddress(key, nextAddress);
            }
            size.decrementAndGet();
            break;
         }
         prevAddress = address;
         address = nextAddress;
      }
      return ice;
   }

   @Override
   public void putAll(Map<? extends WrappedBytes, ? extends InternalCacheEntry<WrappedBytes, WrappedBytes>> m) {
      for (Entry<? extends WrappedBytes, ? extends InternalCacheEntry<WrappedBytes, WrappedBytes>> entry : m.entrySet()) {
         put(entry.getKey(), entry.getValue());
      }
   }

   @Override
   public void clear() {
      locks.lockAll();
      try {
         checkDeallocation();
         if (trace) {
            log.trace("Clearing off heap data");
         }
         memoryLookup.toStreamRemoved().forEach(address -> {
            while (address != 0) {
               long nextAddress = offHeapEntryFactory.getNext(address);
               entryRemoved(address);
               address = nextAddress;
            }
         });
         size.set(0);
         if (trace) {
            log.trace("Cleared off heap data");
         }
      } finally {
         locks.unlockAll();
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> putIfAbsent(WrappedBytes key,
         InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      return compute(key, (k, v) -> {
         if (v == null) {
            return value;
         }
         return v;
      });
   }

   @Override
   public boolean remove(Object key, Object value) {
      if (!(key instanceof WrappedBytes) || !(value instanceof InternalCacheEntry)) {
         return false;
      }
      Object innerValue = ((InternalCacheEntry) value).getValue();
      if (!(innerValue instanceof WrappedBytes)) {
         return false;
      }
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         return address != 0 && performRemove(address, 0, (WrappedBytes) key, (WrappedBytes) innerValue, true) != null;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean replace(WrappedBytes key, InternalCacheEntry<WrappedBytes, WrappedBytes> oldValue,
         InternalCacheEntry<WrappedBytes, WrappedBytes> newValue) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         return address != 0 && performReplace(address, key, oldValue, newValue) != null;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> replace(WrappedBytes key,
         InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         if (address == 0) {
            return null;
         }
         return performReplace(address, key, null, value);
      } finally {
         lock.unlock();
      }
   }

   private InternalCacheEntry<WrappedBytes, WrappedBytes> performReplace(long bucketHeadAddress,
         WrappedBytes key, InternalCacheEntry<WrappedBytes, WrappedBytes> oldValue,
         InternalCacheEntry<WrappedBytes, WrappedBytes> newValue) {
      long prevAddress = 0;
      // We only use the head pointer for the first iteration
      long address = bucketHeadAddress;
      InternalCacheEntry<WrappedBytes, WrappedBytes> ice = null;

      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         // If the actualAddress was not known, check key equality otherwise just compare with the address
         if (offHeapEntryFactory.equalsKey(address, key)) {
            if (oldValue != null) {
               ice = offHeapEntryFactory.fromMemory(address);
               // If value doesn't match and was provided then don't replace it
               if (!ice.getValue().equalsWrappedBytes(oldValue.getValue())) {
                  ice = null;
                  break;
               }
            }
            // Need to always return the previous, so make sure we read it
            if (ice == null) {
               ice = offHeapEntryFactory.fromMemory(address);
            }

            long newAddress = offHeapEntryFactory.create(key, newValue.getValue(), newValue.getMetadata());

            entryReplaced(newAddress, address);
            if (prevAddress != 0) {
               offHeapEntryFactory.setNext(prevAddress, newAddress);
            } else {
               memoryLookup.putMemoryAddress(key, newAddress);
            }
            // We always set the next address on the newly created address - this will be 0 if the previous value
            // was the end of the linked list
            offHeapEntryFactory.setNext(newAddress, nextAddress);
            break;
         }
         prevAddress = address;
         address = nextAddress;
      }
      return ice;
   }

   @Override
   public Set<WrappedBytes> keySet() {
      return new AbstractSet<WrappedBytes>() {
         @Override
         public Iterator<WrappedBytes> iterator() {
            // TODO: should we add a keyStream ?
            return entryStreamIncludingExpired().map(InternalCacheEntry::getKey).iterator();
         }

         @Override
         public int size() {
            return OffHeapConcurrentMap.this.size();
         }

         @Override
         public boolean remove(Object o) {
            return OffHeapConcurrentMap.this.remove(o) != null;
         }
      };
   }

   @Override
   public Collection<InternalCacheEntry<WrappedBytes, WrappedBytes>> values() {
      return new AbstractCollection<InternalCacheEntry<WrappedBytes, WrappedBytes>>() {
         @Override
         public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
            return entryStreamIncludingExpired().iterator();
         }

         @Override
         public int size() {
            return OffHeapConcurrentMap.this.size();
         }

         @Override
         public boolean remove(Object o) {
            return o instanceof InternalCacheEntry && OffHeapConcurrentMap.this.remove(((InternalCacheEntry) o).getKey(),
                     ((InternalCacheEntry) o).getValue());
         }
      };
   }

   private Stream<InternalCacheEntry<WrappedBytes, WrappedBytes>> entryStreamIncludingExpired() {
      int blockSize = memoryAddressCount / lockCount;
      // We only create a stream at a time with this many address lookups - this should be low enough to not cause OOM,
      // but higher as to not cause additional GC pressure from extra streams created
      int blockBatchSize = 256;
      // If we have less than the block size don't do batching and instead just lock entire memory regions at a time
      if (blockSize <= blockBatchSize) {
         // Now memory is segmented into blocks by lock number
         return IntStream.range(0, lockCount)
               .mapToObj(lockNum -> {
                  Lock lock = locks.getLockWithOffset(lockNum).readLock();
                  lock.lock();
                  try {
                     checkDeallocation();
                     Stream.Builder<InternalCacheEntry<WrappedBytes, WrappedBytes>> builder = Stream.builder();
                     for (int offset = 0; offset < blockSize; ++offset) {
                        long address = memoryLookup.getMemoryAddressOffsetNoTraceIfAbsent(blockSize * lockNum + offset);
                        if (address != 0) {
                           long nextAddress;
                           do {
                              nextAddress = offHeapEntryFactory.getNext(address);
                              builder.accept(offHeapEntryFactory.fromMemory(address));
                           } while ((address = nextAddress) != 0);
                        }
                     }
                     return builder.build();
                  } finally {
                     lock.unlock();
                  }
               }).flatMap(Function.identity());
      } else {
         // This branch creates a stream lazily per blockBatchSize worth of address counters - this is useful
         // when address regions is significantly larger than the number of locks in the system

         // The blockSize has to be divisible by the batchSize - this should be guaranteed since memoryAddressCount and
         // lockCount both are powers of two
         int assertionBits = blockBatchSize - 1;
         assert (memoryAddressCount & assertionBits) == 0;
         // We initialize with a stream for each block batch
         return IntStream.range(0, memoryAddressCount / blockBatchSize)
               .mapToObj(blockBatchNum -> {
                  // We figure out the lock by finding how far into the memory space we are and dividing by block
                  // size, since the locks are done contiguously
                  int lockOffset = (blockBatchNum * blockBatchSize) / blockSize;
                  Lock lock = locks.getLockWithOffset(lockOffset).readLock();
                  lock.lock();
                  try {
                     checkDeallocation();
                     Stream.Builder<InternalCacheEntry<WrappedBytes, WrappedBytes>> builder = Stream.builder();
                     for (int offset = 0; offset < blockBatchSize; ++offset) {
                        long address = memoryLookup.getMemoryAddressOffsetNoTraceIfAbsent(blockBatchNum * blockBatchSize + offset);
                        if (address != 0) {
                           long nextAddress;
                           do {
                              nextAddress = offHeapEntryFactory.getNext(address);
                              builder.accept(offHeapEntryFactory.fromMemory(address));
                           } while ((address = nextAddress) != 0);
                        }
                     }
                     return builder.build();
                  } finally {
                     lock.unlock();
                  }
               }).flatMap(Function.identity());
      }
   }

   @Override
   public Set<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>> entrySet() {
      return new AbstractSet<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>>() {
         @Override
         public Iterator<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>> iterator() {
            Stream<Map.Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>> stream = entryStreamIncludingExpired()
                  .map(ice -> new AbstractMap.SimpleImmutableEntry<>(ice.getKey(), ice));
            return stream.iterator();
         }

         @Override
         public int size() {
            return OffHeapConcurrentMap.this.size();
         }
      };
   }
}
