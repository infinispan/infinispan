package org.infinispan.container.offheap;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Data Container implementation that stores entries in native memory (off-heap).
 * @author wburns
 * @since 9.0
 */
public class OffHeapDataContainer implements DataContainer<WrappedBytes, WrappedBytes> {
   protected final Log log = LogFactory.getLog(getClass());
   protected final boolean trace = log.isTraceEnabled();

   protected final AtomicLong size = new AtomicLong();
   protected final int lockCount;
   protected final int memoryAddressCount;
   protected final StripedLock locks;
   protected final MemoryAddressHash memoryLookup;
   protected OffHeapMemoryAllocator allocator;
   protected OffHeapEntryFactory offHeapEntryFactory;
   protected InternalEntryFactory internalEntryFactory;
   protected TimeService timeService;
   protected EvictionManager evictionManager;
   protected PassivationManager passivator;
   // Variable to make sure memory locations aren't read after being deallocated
   // This variable should always be read first after acquiring either the read or write lock
   private boolean dellocated = false;

   // Max would be 1:1 ratio with memory addresses - must be a crazy machine to have that many processors
   private final static int MAX_LOCK_COUNT = 1 << 30;

   static int nextPowerOfTwo(int target) {
      int n = target - 1;
      n |= n >>> 1;
      n |= n >>> 2;
      n |= n >>> 4;
      n |= n >>> 8;
      n |= n >>> 16;
      return (n < 0) ? 1 : n >= MAX_LOCK_COUNT ? MAX_LOCK_COUNT : n + 1;
   }

   public OffHeapDataContainer(int desiredSize) {
      lockCount = nextPowerOfTwo(Runtime.getRuntime().availableProcessors()) << 1;
      int memoryAddresses = desiredSize >= MAX_LOCK_COUNT ? MAX_LOCK_COUNT : lockCount;
      while (memoryAddresses < desiredSize) {
         memoryAddresses <<= 1;
      }
      memoryAddressCount = memoryAddresses;
      memoryLookup = new MemoryAddressHash(memoryAddressCount);
      // Unfortunately desired size directly correlates to lock size
      locks = new StripedLock(lockCount);
   }

   @Inject
   public void inject(EvictionManager evictionManager, PassivationManager passivator, OffHeapEntryFactory offHeapEntryFactory,
                      OffHeapMemoryAllocator allocator, TimeService timeService, InternalEntryFactory internalEntryFactory) {
      this.evictionManager = evictionManager;
      this.passivator = passivator;
      this.internalEntryFactory = internalEntryFactory;
      this.allocator = allocator;
      this.offHeapEntryFactory = offHeapEntryFactory;
      this.timeService = timeService;
   }

   /**
    * Clears the memory lookups and cache data.
    */
   @Stop(priority = Integer.MAX_VALUE)
   public void deallocate() {
      locks.lockAll();
      try {
         if (size.get() != 0) {
            log.warn("Container was not cleared before deallocating memory lookup tables!  Memory leak " +
                  "will have occurred!");
         }
         clear();
         memoryLookup.deallocate();
         dellocated = true;
      } finally {
         locks.unlockAll();
      }
   }

   static WrappedByteArray toWrapper(Object obj) {
      if (obj instanceof WrappedByteArray) {
         return (WrappedByteArray) obj;
      }
      throw new IllegalArgumentException("Require WrappedByteArray: got " + obj.getClass());
   }

   protected void checkDeallocation() {
      if (dellocated) {
         throw new IllegalStateException("Container was already shut down!");
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> get(Object k) {
      Lock lock = locks.getLock(k).readLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(k);
         if (address == 0) {
            return null;
         }

         return performGet(address, k);
      } finally {
         lock.unlock();
      }
   }

   protected InternalCacheEntry<WrappedBytes, WrappedBytes> performGet(long address, Object k) {
      WrappedBytes wrappedKey = toWrapper(k);
      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(address);
         if (wrappedKey.equalsWrappedBytes(ice.getKey())) {
            entryRetrieved(address);
            return ice;
         } else {
            address = nextAddress;
         }
      }
      return null;
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> peek(Object k) {
      return get(k);
   }

   @Override
   public void put(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long newAddress = offHeapEntryFactory.create(key, value, metadata);
         performPut(newAddress, key);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Performs the actual put operation putting the new address into the memory lookups.  The write lock for the given
    * key <b>must</b> be held before calling this method.
    * @param newAddress the address of the new entry
    * @param key the key of the entry
    */
   protected void performPut(long newAddress, WrappedBytes key) {
      long address = memoryLookup.getMemoryAddress(key);
      performPut(address, newAddress, key);
   }

   protected void performPut(long address, long newAddress, WrappedBytes key) {
      boolean shouldCreate = false;
      // Have to start new linked node list
      if (address == 0) {
         memoryLookup.putMemoryAddress(key, newAddress);
         entryCreated(newAddress);
         size.incrementAndGet();
      } else {
         // Whether the key was found or not - short circuit equality checks
         boolean foundKey = false;
         // Holds the previous linked list address
         long prevAddress = 0;
         // Keep looping until we get the tail end - we always append the put to the end
         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            if (!foundKey) {
               if (offHeapEntryFactory.equalsKey(address, key)) {
                  entryReplaced(newAddress, address);
                  allocator.deallocate(address);
                  foundKey = true;
                  // If this is true it means this was the first node in the linked list
                  if (prevAddress == 0) {
                     if (nextAddress == 0) {
                        // This branch is the case where our key is the only one in the linked list
                        shouldCreate = true;
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
         if (!foundKey) {
            entryCreated(newAddress);
            size.incrementAndGet();
         }
         if (shouldCreate) {
            memoryLookup.putMemoryAddress(key, newAddress);
         } else {
            // Now prevAddress should be the last link so we fix our link
            offHeapEntryFactory.setNext(prevAddress, newAddress);
         }
      }
   }

   /**
    * Invoked when an entry is about to be created.  The new address is fully addressable,
    * The write lock will already be acquired for the given * segment the key mapped to.
    * @param newAddress the address just created that will be the new entry
    */
   protected void entryCreated(long newAddress) {

   }

   /**
    * Invoked when an entry is about to be replaced with a new one.  The old and new address are both addressable,
    * however oldAddress may be freed after this method returns.  The write lock will already be acquired for the given
    * segment the key mapped to.
    * @param newAddress the address just created that will be the new entry
    * @param oldAddress the old address for this entry that will be soon removed
    */
   protected void entryReplaced(long newAddress, long oldAddress) {

   }

   /**
    * Invoked when an entry is about to be removed.  You can read values from this but after this method is completed
    * this memory address may be freed. The write lock will already be acquired for the given segment the key mapped to.
    * @param removedAddress the address about to be removed
    */
   protected void entryRemoved(long removedAddress) {

   }

   @Override
   public boolean containsKey(Object k) {
      Lock lock = locks.getLock(k).readLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(k);
         if (address == 0) {
            return false;
         }
         WrappedByteArray wba = toWrapper(k);

         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            if (offHeapEntryFactory.equalsKey(address, wba)) {
               return true;
            }
            address = nextAddress;
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Invoked when an entry is successfully retrieved.  The read lock will already
    * be acquired for the given segment the key mapped to.
    * @param entryAddress
    */
   protected void entryRetrieved(long entryAddress) {

   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> remove(Object key) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         if (address == 0) {
            return null;
         }
         return performRemove(address, key);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Performs the actual remove operation removing the new address from the memory lookups.  The write lock for the given
    * key <b>must</b> be held before calling this method.
    * @param address the address of the entry to remove
    * @param key the key of the entry
    */
   protected InternalCacheEntry<WrappedBytes, WrappedBytes> performRemove(long address, Object key) {
      WrappedByteArray wba = toWrapper(key);
      long prevAddress = 0;

      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(address);
         if (ice.getKey().equals(wba)) {
            entryRemoved(address);
            // Free the node
            allocator.deallocate(address);
            if (prevAddress != 0) {
               offHeapEntryFactory.setNext(prevAddress, nextAddress);
            } else {
               memoryLookup.putMemoryAddress(key, nextAddress);
            }
            size.decrementAndGet();
            return ice;
         }
         prevAddress = address;
         address = nextAddress;
      }
      return null;
   }

   @Override
   public int size() {
      long time = timeService.wallClockTime();
      long count = entryStream().filter(e -> !e.isExpired(time)).count();
      return (int) Math.min(count, Integer.MAX_VALUE);
   }

   @Override
   public int sizeIncludingExpired() {
      return (int) Math.min(size.get(), Integer.MAX_VALUE);
   }

   @Override
   public void clear() {
      locks.lockAll();
      try {
         checkDeallocation();
         performClear();
      } finally {
         locks.unlockAll();
      }
   }

   protected void performClear() {
      if (trace) {
         log.trace("Clearing off heap data");
      }
      memoryLookup.toStreamRemoved().forEach(address -> {
         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            allocator.deallocate(address);
            address = nextAddress;
         }
      });
      size.set(0);
      if (trace) {
         log.trace("Cleared off heap data");
      }
   }

   class ValueCollection extends AbstractCollection<WrappedBytes> {

      @Override
      public Iterator<WrappedBytes> iterator() {
         return stream().iterator();
      }

      @Override
      public void forEach(Consumer<? super WrappedBytes> action) {
         stream().forEach(action);
      }

      @Override
      public Spliterator<WrappedBytes> spliterator() {
         return stream().spliterator();
      }

      @Override
      public Stream<WrappedBytes> stream() {
         return entryStream().map(Map.Entry::getValue);
      }

      @Override
      public Stream<WrappedBytes> parallelStream() {
         return stream().parallel();
      }

      @Override
      public int size() {
         return OffHeapDataContainer.this.size();
      }
   }

   class KeySet extends ValueCollection implements Set<WrappedBytes> {
      @Override
      public Stream<WrappedBytes> stream() {
         return entryStream().map(Map.Entry::getKey);
      }

      @Override
      public boolean contains(Object o) {
         return containsKey(o);
      }
   }

   class EntrySet extends AbstractSet<InternalCacheEntry<WrappedBytes, WrappedBytes>> {

      @Override
      public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
         return stream().iterator();
      }

      @Override
      public int size() {
         return OffHeapDataContainer.this.size();
      }

      @Override
      public void forEach(Consumer<? super InternalCacheEntry<WrappedBytes, WrappedBytes>> action) {
         stream().forEach(action);
      }

      @Override
      public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliterator() {
         return stream().spliterator();
      }

      @Override
      public Stream<InternalCacheEntry<WrappedBytes, WrappedBytes>> stream() {
         return entryStream();
      }

      @Override
      public Stream<InternalCacheEntry<WrappedBytes, WrappedBytes>> parallelStream() {
         return stream().parallel();
      }
   }

   @Override
   public Set<WrappedBytes> keySet() {
      return new KeySet();
   }

   @Override
   public Collection<WrappedBytes> values() {
      return new ValueCollection();
   }

   @Override
   public Set<InternalCacheEntry<WrappedBytes, WrappedBytes>> entrySet() {
      return new EntrySet();
   }

   @Override
   public void evict(WrappedBytes key) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         if (address != 0) {
            // TODO: this could be more efficient
            InternalCacheEntry<WrappedBytes, WrappedBytes> ice = performGet(address, key);
            if (ice != null) {
               passivator.passivate(ice);
               performRemove(address, key);
            }
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key,
         ComputeAction<WrappedBytes, WrappedBytes> action) {
      Lock lock = locks.getLock(key).writeLock();
      lock.lock();
      try {
         checkDeallocation();
         long address = memoryLookup.getMemoryAddress(key);
         InternalCacheEntry<WrappedBytes, WrappedBytes> prev = address == 0 ? null : performGet(address, key);
         InternalCacheEntry<WrappedBytes, WrappedBytes> result = action.compute(key, prev, internalEntryFactory);
         if (prev == result) {
            // noop
         } else if (result != null) {
            long newAddress = offHeapEntryFactory.create(key, result.getValue(), result.getMetadata());
            performPut(address, newAddress, key);
         } else {
            performRemove(address, key);
         }
         return result;
      } finally {
         lock.unlock();
      }
   }

   private void executeTask(Consumer<InternalCacheEntry<WrappedBytes, WrappedBytes>> consumer) {
      for (int i = 0; i < lockCount; ++i) {
         Lock lock = locks.getLockWithOffset(i).readLock();
         lock.lock();
         try {
            checkDeallocation();
            for (int j = i; j < memoryAddressCount; j += lockCount) {
               long address = memoryLookup.getMemoryAddressOffset(j);
               while (address != 0) {
                  long nextAddress = offHeapEntryFactory.getNext(address);
                  InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(address);
                  consumer.accept(ice);
                  address = nextAddress;
               }
            }
         } finally {
            lock.unlock();
         }
      }
   }

   @Override
   public void executeTask(KeyFilter<? super WrappedBytes> filter,
         BiConsumer<? super WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>> action) throws InterruptedException {
      executeTask(ice -> {
         if (filter.accept(ice.getKey())) {
            action.accept(ice.getKey(), ice);
         }
      });
   }

   @Override
   public void executeTask(KeyValueFilter<? super WrappedBytes, ? super WrappedBytes> filter,
         BiConsumer<? super WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>> action) throws InterruptedException {
      executeTask(ice -> {
         if (filter.accept(ice.getKey(), ice.getValue(), ice.getMetadata())) {
            action.accept(ice.getKey(), ice);
         }
      });
   }

   private Stream<InternalCacheEntry<WrappedBytes, WrappedBytes>> entryStream() {
      return IntStream.range(0, memoryAddressCount)
            .mapToObj(a -> {
               Lock lock = locks.getLockWithOffset(a % lockCount).readLock();
               lock.lock();
               try {
                  checkDeallocation();
                  long address = memoryLookup.getMemoryAddressOffset(a);
                  if (address == 0) {
                     return null;
                  }
                  Stream.Builder<InternalCacheEntry<WrappedBytes, WrappedBytes>> builder = Stream.builder();
                  long nextAddress;
                  do {
                     nextAddress = offHeapEntryFactory.getNext(address);
                     builder.accept(offHeapEntryFactory.fromMemory(address));
                  } while ((address = nextAddress) != 0);
                  return builder.build();
               } finally {
                  lock.unlock();
               }
            }).flatMap(Function.identity());
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
      long time = timeService.wallClockTime();
      return entryStream().filter(e -> !e.isExpired(time)).iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iteratorIncludingExpired() {
      return entryStream().iterator();
   }
}
