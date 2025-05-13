package org.infinispan.container.offheap;

import java.lang.invoke.MethodHandles;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.PeekableTouchableMap;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * A {@link ConcurrentMap} implementation that stores the keys and values off the JVM heap in native heap. This map
 * does not permit null for key or values.
 * <p>
 * The key and value are limited to objects that implement the {@link WrappedBytes} interface. Currently this map only allows
 * for implementations that always return a backing array via the {@link WrappedBytes#getBytes()} method.
 * <p>
 * For reference here is a list of commonly used terms:
 * <ul>
 *    <li><code>bucket</code>: Can store multiple entries (normally via a forward only list)
 *    <li><code>memory lookup</code>: Stores an array of buckets - used primarily to lookup the location a key would be
 *    <li><code>lock region</code>: The number of lock regions is fixed, and each region has {@code bucket count / lock count} buckets.
 * </ul>
 * <p>
 * This implementation provides constant-time performance for the basic
 * operations ({@code get}, {@code put}, {@code remove} and {@code compute}), assuming the hash function
 * disperses the elements properly among the buckets.  Iteration over
 * collection views requires time proportional to the number of buckets plus its size (the number
 * of key-value mappings).  This map always assumes a load factor of .75 that is not changeable.
 * <p>
 * A map must be started after creating to create the initial memory lookup, which is also store in the native heap.
 * When the size of the map reaches the load factor, that is .75 times the capacity, the map will attempt to resize
 * by increasing its internal memory lookup to have an array of buckets twice as big. Normal operations can still
 * proceed during this, allowing for minimal downtime during a resize.
 * <p>
 * This map is created assuming some knowledge of expiration in the Infinispan system. Thus operations that do not
 * expose this information via its APIs are not supported. These methods are {@code keySet}, {@code containsKey} and
 * {@code containsValue}.
 * <p>
 * This map guarantees consistency under concurrent read ands writes through a {@link StripedLock} where each
 * {@link java.util.concurrent.locks.ReadWriteLock} instance protects an equivalent region of buckets in the underlying
 * memory lookup. Read operations, that is ones that only acquire the read lock for their specific lock region, are
 * ({@code get} and {@code peek}). Iteration on a returned entrySet or value collection will acquire only a single
 * read lock at a time while inspecting a given lock region for a valid value. Write operations, ones that acquire the
 * write lock for the lock region, are ({@code put}, {@code remove}, {@code replace}, {@code compute}. A clear
 * will acquire all write locks when invoked. This allows the clear to also resize the map down to the initial size.
 * <p>
 * When this map is constructed it is also possible to provide an {@link EntryListener} that is invoked when various
 * operations are performed in the map. Note that the various modification callbacks <b>MUST</b> free the old address,
 * or else a memory leak will occur. Please see the various methods for clarification on these methods.
 * <p>
 * Since this map is based on holding references to memory that lives outside of the scope of the JVM garbage collector
 * users need to ensure they properly invoke the {@link #close()} when the map is no longer in use to properly free
 * all allocated native memory.
 * @author wburns
 * @since 9.4
 */
public class OffHeapConcurrentMap implements ConcurrentMap<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>,
      PeekableTouchableMap<WrappedBytes, WrappedBytes>, AutoCloseable {
   /** Some implementation details
    * <p>
    * All methods that must hold a lock when invoked are annotated with a {@link GuardedBy} annotation. They can have a
    * few different designations, which are described in this table.
    *
    * locks#readLock:  The appropriate read or write lock for the given key must be held when invoking this method.
    * locks#writeLock: The appropriate write lock for the given key must be held when invoking this method.
    * locks#lockAll:   All write locks must be held before invoking this method.
    * locks:           Any read or write lock must be held while reading these - however writes must acquire all write locks.
    */

   /* ---------------- Constants -------------- */

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   // We always have to have more buckets than locks
   public static final int INITIAL_SIZE = 256;

   private static final int LOCK_COUNT = Math.min(Util.findNextHighestPowerOfTwo(ProcessorInfo.availableProcessors()) << 1,
         INITIAL_SIZE);
   // This is the largest power of 2 positive integer value
   private static final int MAX_ADDRESS_COUNT = 1 << 31;
   // Since lockCount is always a power of 2 - We can just shift by this many bits which is the same as dividing by
   // the number of locks
   private static final int LOCK_SHIFT = 31 - Integer.numberOfTrailingZeros(LOCK_COUNT);
   // The number of bits required to shift to the right to get the bucket size from a given pointer address
   private static final int LOCK_REGION_SHIFT = Integer.numberOfTrailingZeros(LOCK_COUNT);

   private final AtomicLong size = new AtomicLong();
   private final StripedLock locks;

   private final OffHeapMemoryAllocator allocator;
   private final OffHeapEntryFactory offHeapEntryFactory;

   private final EntryListener listener;

   // Once this threshold size is met, the underlying buckets will be re-sized if possible
   // This variable can be read outside of locks - thus is volatile, however should only be modified while holding
   // all write locks
   @GuardedBy("locks#lockAll")
   private volatile int sizeThreshold;

   // Non null during a resize operation - this will be initialized to contain all of the numbers equal to how many
   // locks we have - This and oldMemoryLookup should always be either both null or not null at the same time.
   @GuardedBy("locks")
   private IntSet pendingBlocks;
   // Always non null, unless map has been stopped
   @GuardedBy("locks")
   private MemoryAddressHash memoryLookup;
   @GuardedBy("locks")
   private int memoryShift;
   // Non null during a resize operation - this will contain the previous old lookup and may or may not contain valid
   // elements depending upon if a lock region is still pending transfer - This and pendingBlocks should always be
   // either both null or not null at the same time.
   @GuardedBy("locks")
   private MemoryAddressHash oldMemoryLookup;
   @GuardedBy("locks")
   private int oldMemoryShift;

   public OffHeapConcurrentMap(OffHeapMemoryAllocator allocator,
         OffHeapEntryFactory offHeapEntryFactory, EntryListener listener) {
      this.allocator = Objects.requireNonNull(allocator);
      this.offHeapEntryFactory = Objects.requireNonNull(offHeapEntryFactory);
      this.listener = listener;

      locks = new StripedLock(LOCK_COUNT);

      locks.lockAll();
      try {
         if (!sizeMemoryBuckets(INITIAL_SIZE)) {
            throw new IllegalArgumentException("Unable to initialize off-heap addresses as memory eviction is too low!");
         }
      } finally {
         locks.unlockAll();
      }
   }

   @Override
   public boolean touchKey(Object k, long currentTimeMillis) {
      if (!(k instanceof WrappedBytes)) {
         return false;
      }
      int hashCode = k.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      // We need the write lock as we may have to replace the value entirely
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         MemoryAddressHash memoryLookup;
         if (pendingBlocks != null && pendingBlocks.contains(lockOffset)) {
            memoryLookup = this.oldMemoryLookup;
         } else {
            memoryLookup = this.memoryLookup;
         }
         return lockedTouch(memoryLookup, (WrappedBytes) k, hashCode, currentTimeMillis);
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
   }

   @Override
   public void touchAll(long currentTimeMillis) {
      // TODO: eventually optimize this to not create object instances and just touch memory directly
      // but requires additional rewrite as we need to ensure this is done with a write lock
      Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator = entryIterator();
      while (iterator.hasNext()) {
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = iterator.next();
         touchKey(ice.getKey(), currentTimeMillis);
      }
   }

   @GuardedBy("locks#writeLock")
   private boolean lockedTouch(MemoryAddressHash memoryLookup, WrappedBytes k, int hashCode, long currentTimeMillis) {
      int memoryOffset = getMemoryOffset(memoryLookup, hashCode);
      long bucketAddress = memoryLookup.getMemoryAddressOffset(memoryOffset);
      if (bucketAddress == 0) {
         return false;
      }

      long actualAddress = performGet(bucketAddress, k, hashCode);
      if (actualAddress != 0) {
         long newAddress = offHeapEntryFactory.updateMaxIdle(actualAddress, currentTimeMillis);
         if (newAddress != 0) {
            // Replaces the old value with the newly created one
            performPut(bucketAddress, actualAddress, newAddress, k, memoryOffset, false, false);
         } else {
            entryRetrieved(actualAddress);
         }
         return true;
      }
      return false;
   }

   /**
    * Listener interface that is notified when certain operations occur for various memory addresses. Note that when
    * this listener is used certain operations are not performed and require the listener to do these instead. Please
    * note each method documentation to tell what those are.
    */
   public interface EntryListener {
      /**
       * Invoked when a resize event occurs. This will be invoked up to two times: once for the new container with
       * a positive count and a possibly a second time for the now old container with a negative count. Note that
       * the pointers are in a single contiguous block. It is possible to prevent the resize by returning false
       * from the invocation.
       * @param pointerCount the change in pointers
       * @return whether the resize should continue
       */
      boolean resize(int pointerCount);

      /**
       * Invoked when an entry is about to be created.  The new address is fully addressable,
       * The write lock will already be acquired for the given segment the key mapped to.
       * @param newAddress the address just created that will be the new entry
       */
      void entryCreated(long newAddress);

      /**
       * Invoked when an entry is about to be removed.  You can read values from this but after this method is completed
       * this memory address may be freed. The write lock will already be acquired for the given segment the key mapped to.
       * @param removedAddress the address about to be removed
       */
      void entryRemoved(long removedAddress);

      /**
       * Invoked when an entry is about to be replaced with a new one.  The old and new address are both addressable,
       * however oldAddress may be freed after this method returns.  The write lock will already be acquired for the given
       * segment the key mapped to.
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

   @GuardedBy("locks#writeLock")
   private void entryCreated(long newAddress) {
      if (listener != null) {
         listener.entryCreated(newAddress);
      }
   }

   @GuardedBy("locks#writeLock")
   private void entryRemoved(long removedAddress) {
      if (listener != null) {
         listener.entryRemoved(removedAddress);
      }
      allocator.deallocate(removedAddress, offHeapEntryFactory.getSize(removedAddress, false));
   }

   @GuardedBy("locks#writeLock")
   private void entryReplaced(long newAddress, long oldAddress) {
      if (listener != null) {
         listener.entryReplaced(newAddress, oldAddress);
      }
      allocator.deallocate(oldAddress, offHeapEntryFactory.getSize(oldAddress, false));
   }

   @GuardedBy("locks#readLock")
   private void entryRetrieved(long entryAddress) {
      if (listener != null) {
         listener.entryRetrieved(entryAddress);
      }
   }

   private static int spread(int h) {
      // Spread using fibonacci hash (using golden ratio)
      // This number is ((2^31 -1) / 1.61803398875) - then rounded to nearest odd number
      // We want something that will prevent hashCodes that are near each other being in the same bucket but still fast
      // We then force the number to be positive by throwing out the first bit
      return (h * 1327217885) & Integer.MAX_VALUE;
   }

   /**
    * Returns the bucket offset calculated from the provided hashCode for the current memory lookup.
    * @param hashCode hashCode of the key to find the bucket offset for
    * @return offset to use in the memory lookup
    */
   @GuardedBy("locks#readLock")
   private int getMemoryOffset(int hashCode) {
      return getOffset(hashCode, memoryShift);
   }

   private int getOffset(int hashCode, int shift) {
      return spread(hashCode) >>> shift;
   }

   /**
    * Returns the bucket offset calculated from the provided hashCode for the provided memory lookup.
    * @param hashCode hashCode of the key to find the bucket offset for
    * @return offset to use in the memory lookup
    */
   @GuardedBy("locks#readLock")
   private int getMemoryOffset(MemoryAddressHash memoryLookup, int hashCode) {
      return getOffset(hashCode, memoryLookup == this.memoryLookup ? memoryShift : oldMemoryShift);
   }

   StampedLock getStampedLock(int hashCode) {
      return locks.getLockWithOffset(getLockOffset(hashCode));
   }

   private int getLockOffset(int hashCode) {
      return getOffset(hashCode, LOCK_SHIFT);
   }

   /**
    * Returns how large a region of buckets (that is how many buckets a single lock protects). The returned number
    * will always be less than or equal to the provided <b>bucketTotal</b>.
    * @param bucketTotal number of buckets
    * @return how many buckets map to a lock region
    */
   private int getBucketRegionSize(int bucketTotal) {
      return bucketTotal >>> LOCK_REGION_SHIFT;
   }

   private void checkDeallocation() {
      if (memoryLookup == null) {
         throw new IllegalStateException("Map was already shut down!");
      }
   }

   /**
    * Expands the memory buckets if possible, returning if it was successful.
    * If it was unable to expand the bucket array, it will set the sizeThreshold to MAX_VALUE to prevent future
    * attempts to resize the container
    * @param bucketCount the expected new size
    * @return true if the bucket was able to be resized
    */
   @GuardedBy("locks#lockAll")
   private boolean sizeMemoryBuckets(int bucketCount) {
      if (listener != null) {
         if (!listener.resize(bucketCount)) {
            sizeThreshold = Integer.MAX_VALUE;
            return false;
         }
      }

      sizeThreshold = computeThreshold(bucketCount);

      oldMemoryLookup = memoryLookup;
      oldMemoryShift = memoryShift;
      memoryLookup = new MemoryAddressHash(bucketCount, allocator);
      // Max capacity is 2^31 (thus find the bit position that would be like dividing evenly into that)
      memoryShift = 31 - Integer.numberOfTrailingZeros(bucketCount);

      return true;
   }

   /**
    * Computes the threshold for when a resize should occur. The returned value will be 75% of provided number, assuming
    * it is a power of two (provides a .75 load factor)
    * @param bucketCount the current bucket size
    * @return the resize threshold to use
    */
   static int computeThreshold(int bucketCount) {
      return bucketCount - (bucketCount >> 2);
   }

   StripedLock getLocks() {
      return locks;
   }

   /**
    * This method checks if the map must be resized and if so starts the operation. This caller <b>MUST NOT</b>
    * hold any locks when invoked.
    */
   private void checkResize() {
      // We don't do a resize if we aren't to the boundary or if we are in a pending resize
      if (size.get() < sizeThreshold || oldMemoryLookup != null) {
         return;
      }
      boolean onlyHelp = false;
      IntSet localPendingBlocks;
      locks.lockAll();
      try {
         // Don't replace blocks if it was already done - means we had concurrent requests
         if (oldMemoryLookup != null) {
            onlyHelp = true;
            localPendingBlocks = this.pendingBlocks;
         } else {
            int newBucketCount = memoryLookup.getPointerCount() << 1;
            if (newBucketCount == MAX_ADDRESS_COUNT) {
               sizeThreshold = Integer.MAX_VALUE;
            }

            // We couldn't resize
            if (!sizeMemoryBuckets(newBucketCount)) {
               return;
            }
            localPendingBlocks = IntSets.concurrentSet(LOCK_COUNT);
            for (int i = 0; i < LOCK_COUNT; ++i) {
               localPendingBlocks.set(i);
            }
            this.pendingBlocks = localPendingBlocks;
         }
      } finally {
         locks.unlockAll();
      }

      // Try to complete without waiting if possible for locks
      helpCompleteTransfer(localPendingBlocks, true);

      if (!onlyHelp) {
         if (!localPendingBlocks.isEmpty()) {
            // We attempted to transfer without waiting on locks - but we didn't finish them all yet - so now we have
            // to wait to ensure they are all transferred
            helpCompleteTransfer(localPendingBlocks, false);
            // Now everything should be empty for sure
            assert localPendingBlocks.isEmpty();
         }

         // Now that all blocks have been transferred we can replace references
         locks.lockAll();
         try {
            // This means that someone else completed the transfer for us - only clear can do that currently
            if (this.pendingBlocks == null) {
               return;
            }
            transferComplete();
         } finally {
            locks.unlockAll();
         }
      }
   }

   /**
    * Invoked when a transfer has completed to clean up the old memory lookup
    */
   @GuardedBy("locks#lockAll")
   private void transferComplete() {
      MemoryAddressHash oldMemoryLookup = this.oldMemoryLookup;
      this.pendingBlocks = null;
      if (listener != null) {
         boolean resized = listener.resize(-oldMemoryLookup.getPointerCount());
         assert resized : "Resize of negative pointers should always work!";
      }
      this.oldMemoryLookup = null;

      oldMemoryLookup.deallocate();
   }

   /**
    * This <b>MUST NOT</b>  be invoked while holding any lock
    * @param tryLock whether the lock acquisition only does a try, returning earlier with some lock segments not transferred possibly
    */
   private void helpCompleteTransfer(IntSet pendingBlocks, boolean tryLock) {
      if (pendingBlocks != null) {
         PrimitiveIterator.OfInt iterator = pendingBlocks.iterator();
         while (iterator.hasNext()) {
            int offset = iterator.nextInt();
            StampedLock lock = locks.getLockWithOffset(offset);

            long stamp;
            if (tryLock) {
               // If we can't get it - just assume another person is working on it - so try next one
               if ((stamp = lock.tryWriteLock()) == 0) {
                  continue;
               }
            } else {
               stamp = lock.writeLock();
            }
            try {
               // Only run it now that we have lock if someone else just didn't finish it
               if (pendingBlocks.remove(offset)) {
                  transfer(offset);
               }
            } finally {
               lock.unlockWrite(stamp);
            }
         }
      }
   }

   /**
    * Ensures that the block that maps to the given lock offset is transferred. This method <b>MUST</b> be invoked by
    * any write operation before doing anything. This ensures that the write operation only needs to modify the
    * current memory lookup.
    * @param lockOffset the lock offset to confirm has been transferred
    */
   @GuardedBy("locks#writeLock")
   private void ensureTransferred(int lockOffset) {
      if (pendingBlocks != null) {
         if (pendingBlocks.remove(lockOffset)) {
            transfer(lockOffset);
         }
      }
   }

   /**
    * Transfers all the entries that map to the given lock offset position from the old lookup to the current one.
    * @param lockOffset the offset in the lock array - this is the same between all memory lookups
    */
   @GuardedBy("locks#writeLock")
   private void transfer(int lockOffset) {
      int pointerCount = oldMemoryLookup.getPointerCount();
      int blockSize = getBucketRegionSize(pointerCount);

      LongStream memoryLocations = oldMemoryLookup.removeAll(lockOffset * blockSize, blockSize);
      memoryLocations.forEach(address -> {
         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            offHeapEntryFactory.setNext(address, 0);

            int hashCode = offHeapEntryFactory.getHashCode(address);
            int memoryOffset = getMemoryOffset(hashCode);
            long newBucketAddress = memoryLookup.getMemoryAddressOffset(memoryOffset);

            // We should be only inserting a new value - thus we don't worry about key or return value
            performPut(newBucketAddress, address, address, null, memoryOffset,false, true);

            address = nextAddress;
         }
      });
   }

   @Override
   public void close() {
      locks.lockAll();
      try {
         actualClear();
         memoryLookup.deallocate();
         memoryLookup = null;
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
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      InternalCacheEntry<WrappedBytes, WrappedBytes> result;
      InternalCacheEntry<WrappedBytes, WrappedBytes> prev;
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();

         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long bucketAddress = memoryLookup.getMemoryAddressOffset(memoryOffset);
         long actualAddress = bucketAddress == 0 ? 0 : performGet(bucketAddress, key, hashCode);
         if (actualAddress != 0) {
            prev = offHeapEntryFactory.fromMemory(actualAddress);
         } else {
            prev = null;
         }
         result = remappingFunction.apply(key, prev);
         if (prev == result) {
            // noop
         } else if (result != null) {
            long newAddress = offHeapEntryFactory.create(key, hashCode, result);
            // TODO: Technically actualAddress could be a 0 and bucketAddress != 0, which means we will loop through
            // entire bucket for no reason as it will never match (doing key equality checks)
            performPut(bucketAddress, actualAddress, newAddress, key, memoryOffset, false, false);
         } else {
            // result is null here - so we remove the entry
            performRemove(bucketAddress, actualAddress, key, null, memoryOffset, false);
         }
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
      if (prev == null && result != null) {
         checkResize();
      }
      return result;
   }

   @Override
   public boolean containsKey(Object key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException();
   }

   private InternalCacheEntry<WrappedBytes, WrappedBytes> peekOrGet(WrappedBytes k, boolean peek) {
      int hashCode = k.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long readStamp = stampedLock.readLock();
      try {
         checkDeallocation();
         MemoryAddressHash memoryLookup;
         if (pendingBlocks != null && pendingBlocks.contains(lockOffset)) {
            memoryLookup = this.oldMemoryLookup;
         } else {
            memoryLookup = this.memoryLookup;
         }
         return lockedPeekOrGet(memoryLookup, k, hashCode, peek);
      } finally {
         stampedLock.unlockRead(readStamp);
      }
   }

   @GuardedBy("locks#readLock")
   private InternalCacheEntry<WrappedBytes, WrappedBytes> lockedPeekOrGet(MemoryAddressHash memoryLookup,
         WrappedBytes k, int hashCode, boolean peek) {
      long bucketAddress = memoryLookup.getMemoryAddressOffset(getMemoryOffset(memoryLookup, hashCode));
      if (bucketAddress == 0) {
         return null;
      }

      long actualAddress = performGet(bucketAddress, k, hashCode);
      if (actualAddress != 0) {
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = offHeapEntryFactory.fromMemory(actualAddress);
         if (!peek) {
            entryRetrieved(actualAddress);
         }
         return ice;
      }
      return null;
   }

   /**
    * Gets the actual address for the given key in the given bucket or 0 if it isn't present or expired
    * @param bucketHeadAddress the starting address of the bucket
    * @param k the key to retrieve the address for it if matches
    * @return the address matching the key or 0
    */
   @GuardedBy("locks#readLock")
   private long performGet(long bucketHeadAddress, WrappedBytes k, int hashCode) {
      long address = bucketHeadAddress;
      while (address != 0) {
         long nextAddress = offHeapEntryFactory.getNext(address);
         if (offHeapEntryFactory.equalsKey(address, k, hashCode)) {
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
   public void putNoReturn(WrappedBytes key, InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         long newAddress = offHeapEntryFactory.create(key, hashCode, value);
         performPut(address, 0, newAddress, key, memoryOffset, false, false);
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
      checkResize();
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> put(WrappedBytes key,
         InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      InternalCacheEntry<WrappedBytes, WrappedBytes> returnedValue;
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         long newAddress = offHeapEntryFactory.create(key, hashCode, value);
         returnedValue = performPut(address, 0, newAddress, key, memoryOffset, true, false);
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
      // If we added a new entry, check the resize
      if (returnedValue == null) {
         checkResize();
      }
      return returnedValue;
   }

   /**
    * Performs the actual put operation, adding the new address into the memoryOffset bucket
    * and possibly removing the old entry with the same key.
    * Always adds the new entry at the end of the bucket's linked list.
    * @param bucketHeadAddress the entry address of the first element in the lookup
    * @param actualAddress the actual address if it is known or 0. By passing this != 0 equality checks can be bypassed.
    *                      If a value of 0 is provided this will use key equality.
    * @param newAddress the address of the new entry
    * @param key the key of the entry
    * @param requireReturn whether the return value is required
    * @return {@code true} if the entry doesn't exists in memory and was newly create, {@code false} otherwise
    */
   @GuardedBy("locks#writeLock")
   private InternalCacheEntry<WrappedBytes, WrappedBytes> performPut(long bucketHeadAddress, long actualAddress,
         long newAddress, WrappedBytes key, int memoryOffset, boolean requireReturn, boolean transfer) {
      // Have to start new linked node list
      if (bucketHeadAddress == 0) {
         memoryLookup.putMemoryAddressOffset(memoryOffset, newAddress);
         if (!transfer) {
            entryCreated(newAddress);
            size.incrementAndGet();
         }
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
                  assert !transfer : "We should never have a replace with put from a transfer!";
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
                        memoryLookup.putMemoryAddressOffset(memoryOffset, nextAddress);
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
         if (!foundPrevious && !transfer) {
            entryCreated(newAddress);
            size.incrementAndGet();
         }
         if (replaceHead) {
            memoryLookup.putMemoryAddressOffset(memoryOffset, newAddress);
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
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         if (address == 0) {
            return null;
         }
         return performRemove(address, 0, (WrappedBytes) key, null, memoryOffset,true);
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
   }

   /**
    * This method is designed to be called by an outside class. The write lock for the given key must
    * be acquired via the lock returned from {@link #getStampedLock(int)} using the key's hash code.
    * This method will avoid some additional lookups as the memory address is already acquired and not return
    * the old entry.
    * @param key key to remove
    * @param address the address for the key
    */
   @GuardedBy("locks#writeLock")
   void remove(WrappedBytes key, long address) {
      int hashCode = key.hashCode();
      ensureTransferred(getLockOffset(hashCode));

      int memoryOffset = getMemoryOffset(hashCode);
      long bucketAddress = memoryLookup.getMemoryAddressOffset(memoryOffset);
      assert bucketAddress != 0;
      performRemove(bucketAddress, address, key, null, memoryOffset, false);
   }

   /**
    * Performs the actual remove operation removing the new address from its appropriate bucket.
    * @param bucketHeadAddress the starting address of the bucket
    * @param actualAddress the actual address if it is known or 0. By passing this != 0 equality checks can be bypassed.
    *                      If a value of 0 is provided this will use key equality. key is not required when this != 0
    * @param key the key of the entry
    * @param value the value to match if present
    * @param memoryOffset the offset in the memory bucket where this key mapped to
    * @param requireReturn whether this method is forced to return the entry removed (optimizations can be done if
    *                      the entry is not needed)
    */
   @GuardedBy("locks#writeLock")
   private InternalCacheEntry<WrappedBytes, WrappedBytes> performRemove(long bucketHeadAddress, long actualAddress,
         WrappedBytes key, WrappedBytes value, int memoryOffset, boolean requireReturn) {
      long prevAddress = 0;
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
               memoryLookup.putMemoryAddressOffset(memoryOffset, nextAddress);
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
         actualClear();
      } finally {
         locks.unlockAll();
      }
   }

   @GuardedBy("locks#lockAll")
   private void actualClear() {
      checkDeallocation();
      if (log.isTraceEnabled()) {
         log.trace("Clearing off-heap data");
      }
      LongConsumer removeEntries = address -> {
         while (address != 0) {
            long nextAddress = offHeapEntryFactory.getNext(address);
            entryRemoved(address);
            address = nextAddress;
         }
      };
      int pointerCount = memoryLookup.getPointerCount();
      memoryLookup.removeAll().forEach(removeEntries);
      memoryLookup.deallocate();
      memoryLookup = null;
      if (listener != null) {
         boolean resized = listener.resize(-pointerCount);
         assert resized : "Resize of negative pointers should always work!";
      }
      if (oldMemoryLookup != null) {
         oldMemoryLookup.removeAll().forEach(removeEntries);
         transferComplete();
      }

      // Initialize to beginning again
      sizeMemoryBuckets(INITIAL_SIZE);

      size.set(0);
      if (log.isTraceEnabled()) {
         log.trace("Cleared off-heap data");
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
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         return address != 0 && performRemove(address, 0, (WrappedBytes) key, (WrappedBytes) innerValue, memoryOffset, true) != null;
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
   }

   @Override
   public boolean replace(WrappedBytes key, InternalCacheEntry<WrappedBytes, WrappedBytes> oldValue,
         InternalCacheEntry<WrappedBytes, WrappedBytes> newValue) {
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         return address != 0 && performReplace(address, key, hashCode, memoryOffset, oldValue, newValue) != null;
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> replace(WrappedBytes key,
         InternalCacheEntry<WrappedBytes, WrappedBytes> value) {
      int hashCode = key.hashCode();
      int lockOffset = getLockOffset(hashCode);
      StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
      long writeStamp = stampedLock.writeLock();
      try {
         checkDeallocation();
         ensureTransferred(lockOffset);

         int memoryOffset = getMemoryOffset(hashCode);
         long address = memoryLookup.getMemoryAddressOffset(memoryOffset);
         if (address == 0) {
            return null;
         }
         return performReplace(address, key, hashCode, memoryOffset, null, value);
      } finally {
         stampedLock.unlockWrite(writeStamp);
      }
   }

   /**
    * Performs the actual replace operation removing the old entry and if removed writes the new entry into the same
    * bucket.
    * @param bucketHeadAddress the starting address of the bucket
    * @param key the key of the entry
    * @param hashCode the hasCode of the key
    * @param memoryOffset the offset in the memory bucket where this key mapped to
    * @param oldValue optional old value to match against - if null then any value will be replaced
    * @param newValue new value to place into the map replacing the old if possible
    * @return replaced value or null if the entry wasn't present
    */
   @GuardedBy("locks#writeLock")
   private InternalCacheEntry<WrappedBytes, WrappedBytes> performReplace(long bucketHeadAddress, WrappedBytes key,
         int hashCode, int memoryOffset, InternalCacheEntry<WrappedBytes, WrappedBytes> oldValue,
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

            long newAddress = offHeapEntryFactory.create(key, hashCode, newValue);

            entryReplaced(newAddress, address);
            if (prevAddress != 0) {
               offHeapEntryFactory.setNext(prevAddress, newAddress);
            } else {
               memoryLookup.putMemoryAddressOffset(memoryOffset, newAddress);
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
      throw new UnsupportedOperationException("keySet is not supported as it doesn't contain expiration data");
   }

   @Override
   public Collection<InternalCacheEntry<WrappedBytes, WrappedBytes>> values() {
      return new AbstractCollection<InternalCacheEntry<WrappedBytes, WrappedBytes>>() {
         @Override
         public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
            return entryIterator();
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

   /**
    * Stateful iterator implementation that works by going through the underlying buckets one by one until it finds
    * a non empty bucket. It will then store the values from that bucket to be returned via the {@code next} method.
    * <p>
    * When the iterator is used without a resize the operation is pretty straight forward as it will keep
    * an offset into the buckets and continually reading the next and acquiring the appropriate read lock for that
    * bucket location.
    * <p>
    * During a resize, iteration can be a bit more interesting. If a resize occurs when an iteration is ongoing it
    * can cause the iteration to change to change behavior temporarily. If the iteration is in the middle of iterating
    * over a lock region and that region is resized it must now extrapolate given by the size of the increase of size
    * which buckets the corresponding resize have moved to. Luckily this operation is still efficient as resized buckets
    * are stored contiguously.
    */
   private class ValueIterator implements Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> {
      int bucketPosition;
      // -1 symbolizes it is the first time the iterator is used
      int bucketCount = -1;
      int bucketLockShift;
      int bucketLockStop;

      Queue<InternalCacheEntry<WrappedBytes, WrappedBytes>> values = new ArrayDeque<>();

      @Override
      public boolean hasNext() {
         if (!values.isEmpty()) {
            return true;
         }
         checkAndReadBucket();
         return !values.isEmpty();
      }

      private void checkAndReadBucket() {
         while (bucketPosition != bucketCount) {
            if (readNextBucket()) {
               break;
            }
         }
      }

      @Override
      public InternalCacheEntry<WrappedBytes, WrappedBytes> next() {
         InternalCacheEntry<WrappedBytes, WrappedBytes> ice = values.poll();
         if (ice == null) {
            // Caller invoked next without checking hasNext - try to see if anything is available
            checkAndReadBucket();
            ice = values.remove();
         }
         return ice;
      }

      /**
       * Reads buckets until it finds one that is not empty or it finishes reading a lock region. This method is meant
       * to be invoked multiple times changing iteration state on each call.
       * @return whether a value has been read
       */
      boolean readNextBucket() {
         boolean foundValue = false;
         int lockOffset = getLockOffset(bucketPosition);
         StampedLock stampedLock = locks.getLockWithOffset(lockOffset);
         long readStamp = stampedLock.readLock();
         try {
            checkDeallocation();
            MemoryAddressHash memoryAddressHash;
            if (pendingBlocks != null && pendingBlocks.contains(lockOffset)) {
               memoryAddressHash = oldMemoryLookup;
            } else {
               memoryAddressHash = memoryLookup;
            }
            int pointerCount = memoryAddressHash.getPointerCount();
            if (bucketCount == -1) {
               bucketCount = pointerCount;
               bucketLockStop = getBucketRegionSize(bucketCount);
               bucketLockShift = Integer.numberOfTrailingZeros(bucketLockStop);
            } else if (bucketCount > pointerCount) {
               // If bucket count is greater than pointer count - it means we had a clear in the middle of iterating
               // Just return without adding anymore values
               bucketPosition = bucketCount;
               return false;
            } else if (bucketCount < pointerCount) {
               resizeIteration(pointerCount);
            }
            boolean completedLockBucket;
            // Normal iteration just keep adding entries until either we complete the lock bucket region or
            // we read bytes over the read threshold
            while (!(completedLockBucket = bucketLockStop == bucketPosition)) {
               long address = memoryAddressHash.getMemoryAddressOffsetNoTraceIfAbsent(bucketPosition++);
               if (address != 0) {
                  long nextAddress;
                  do {
                     nextAddress = offHeapEntryFactory.getNext(address);
                     values.add(offHeapEntryFactory.fromMemory(address));
                     foundValue = true;
                  } while ((address = nextAddress) != 0);
                  // We read a single bucket now return to get the value back
                  break;
               }
            }
            // If we completed the lock region and we haven't yet gone through the all buckets, we have to
            // prepare for the next lock region worth of buckets
            if (completedLockBucket && bucketPosition != bucketCount) {
               bucketLockStop += getBucketRegionSize(bucketCount);
            }
         } finally {
            stampedLock.unlockRead(readStamp);
         }
         return foundValue;
      }

      /**
       * Invoked when the iteration saw a bucket size less than the current bucket size of the memory lookup. This
       * means we had a resize during iteration. We must update our bucket position, stop and counts properly based
       * on how many resizes have occurred.
       * @param newBucketSize how large the new bucket size is
       */
      @GuardedBy("locks#readLock")
      private void resizeIteration(int newBucketSize) {
         int bucketIncreaseShift = 31 - Integer.numberOfTrailingZeros(bucketCount) - memoryShift;

         bucketPosition = bucketPosition << bucketIncreaseShift;
         bucketLockStop = bucketLockStop << bucketIncreaseShift;
         bucketLockShift = Integer.numberOfTrailingZeros(bucketLockStop);
         bucketCount = newBucketSize;
      }

      private int getLockOffset(int bucketPosition) {
         return bucketPosition >>> bucketLockShift;
      }
   }

   private Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> entryIterator() {
      if (size.get() == 0) {
         return Collections.emptyIterator();
      }
      return new ValueIterator();
   }

   @Override
   public Set<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>> entrySet() {
      return new AbstractSet<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>>() {
         @Override
         public Iterator<Entry<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>>> iterator() {
            Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> entryIterator = entryIterator();
            return new IteratorMapper<>(entryIterator, ice -> new AbstractMap.SimpleImmutableEntry<>(ice.getKey(), ice));
         }

         @Override
         public int size() {
            return OffHeapConcurrentMap.this.size();
         }
      };
   }
}
