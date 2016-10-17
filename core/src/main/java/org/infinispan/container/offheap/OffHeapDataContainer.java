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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import sun.misc.Unsafe;

/**
 * @author wburns
 * @since 9.0
 */
public class OffHeapDataContainer implements DataContainer<WrappedBytes, WrappedBytes> {
   private static final Log log = LogFactory.getLog(OffHeapDataContainer.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   private final AtomicLong size = new AtomicLong();
   private final int lockCount;
   private final int memoryAddressCount;
   private final StripedLock locks;
   private final MemoryAddressHash memoryLookup;
   private OffHeapMemoryAllocator allocator;
   private OffHeapEntryFactory offHeapEntryFactory;
   private InternalEntryFactory internalEntryFactory;
   private TimeService timeService;
   private PassivationManager passivator;
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
   public void inject(PassivationManager passivator, OffHeapEntryFactory offHeapEntryFactory,
         OffHeapMemoryAllocator allocator, TimeService timeService, InternalEntryFactory internalEntryFactory) {
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

   private void checkDeallocation() {
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

         WrappedBytes wrappedKey = toWrapper(k);
         while (address != 0) {
            long nextAddress = UNSAFE.getLong(address);
            long realAddress = address + 8;

            InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(realAddress);
            if (wrappedKey.equalsWrappedBytes(ice.getKey())) {
               return ice;
            } else {
               address = nextAddress;
            }
         }
         return null;
      } finally {
         lock.unlock();
      }
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

         long address = memoryLookup.getMemoryAddress(key);
         boolean shouldCreate = false;
         // Have to start new linked node list
         if (address == 0) {
            memoryLookup.putMemoryAddress(key, newAddress);
         } else {
            // Whether the key was found or not - short circuit equality checks
            boolean foundKey = false;
            // Holds the previous linked list address
            long prevAddress = 0;
            // Keep looping until we get the tail end - we always append the put to the end
            while (address != 0) {
               long nextAddress = UNSAFE.getLong(address);
               long realAddress = address + 8;
               if (!foundKey) {
                  if (offHeapEntryFactory.equalsKey(realAddress, key)) {
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
                        UNSAFE.putLong(prevAddress, nextAddress);
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
            if (!foundKey) {
               size.incrementAndGet();
            }
            if (shouldCreate) {
               memoryLookup.putMemoryAddress(key, newAddress);
            } else {
               // Now prevAddress should be the last link so we fix our link
               UNSAFE.putLong(prevAddress, newAddress);
            }
         }
      } finally {
         lock.unlock();
      }
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
            long nextAddress = UNSAFE.getLong(address);
            long realAddress = address + 8;
            if (offHeapEntryFactory.equalsKey(realAddress, wba)) {
               return true;
            }
            address = nextAddress;
         }
         return false;
      } finally {
         lock.unlock();
      }
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
         WrappedByteArray wba = toWrapper(key);
         long prevAddress = 0;

         while (address != 0) {
            long nextAddress = UNSAFE.getLong(address);
            long realAddress = address + 8;

            InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(realAddress);
            if (ice.getKey().equals(wba)) {
               // Free the node
               allocator.deallocate(address);
               if (prevAddress != 0) {
                  UNSAFE.putLong(prevAddress, nextAddress);
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
      } finally {
         lock.unlock();
      }
   }

   @Override
   public int size() {
      long time = timeService.time();
      long count = entryStream().filter(e -> !e.isExpired(time)).count();
      if (count > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return (int) count;
   }

   @Override
   public int sizeIncludingExpired() {
      long currentSize = size.get();
      if (currentSize > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return (int) currentSize;
   }

   @Override
   public void clear() {
      locks.lockAll();
      try {
         checkDeallocation();
         memoryLookup.toStreamRemoved().forEach(address -> {
            while (address != 0) {
               long nextAddress = UNSAFE.getLong(address);
               allocator.deallocate(address);
               address = nextAddress;
            }
         });
         size.set(0);
      } finally {
         locks.unlockAll();
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
         // TODO: this could be more efficient
         passivator.passivate(get(key));
         remove(key);
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
         InternalCacheEntry<WrappedBytes, WrappedBytes> prev = get(key);
         InternalCacheEntry<WrappedBytes, WrappedBytes> result = action.compute(key, prev, internalEntryFactory);
         if (result != null) {
            // Could be more efficient
            put(result.getKey(), result.getValue(), result.getMetadata());
         } else {
            remove(key);
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
                  long nextAddress = UNSAFE.getLong(address);
                  long realAddress = address + 8;
                  InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(realAddress);
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
      int limit = memoryAddressCount / lockCount;
      return IntStream.range(0, lockCount)
            // REALLY REALLY stupid there is no flatMapToObj on IntStream...
            .boxed()
            .flatMap(l -> {
               int value = l;
               return LongStream.iterate(value, i -> i + lockCount).limit(limit)
                     .boxed()
                     .flatMap(a -> {
                     Lock lock = locks.getLockWithOffset(value).readLock();
                     lock.lock();
                     try {
                        checkDeallocation();
                        long address = memoryLookup.getMemoryAddressOffset(a.intValue());
                        if (address == 0) {
                           return Stream.empty();
                        }
                        Stream.Builder<InternalCacheEntry<WrappedBytes, WrappedBytes>> builder = Stream.builder();
                        while (address != 0) {
                           long nextAddress;
                           do {
                              nextAddress = UNSAFE.getLong(address);
                              long realAddress = address + 8;
                              builder.accept(offHeapEntryFactory.fromMemory(realAddress));
                           } while ((address = nextAddress) != 0);
                        }
                        return builder.build();
                     } finally {
                        lock.unlock();
                     }
                  });
            });
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
      long time = timeService.time();
      return entryStream().filter(e -> !e.isExpired(time)).iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iteratorIncludingExpired() {
      return entryStream().iterator();
   }
}
