package org.infinispan.container.offheap;

import java.util.stream.LongStream;

import sun.misc.Unsafe;

/**
 * @author wburns
 * @since 9.0
 */
public class MemoryAddressHash {
   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;
   private static final OffHeapMemory MEMORY = OffHeapMemory.INSTANCE;
   private static final int MAXIMUM_CAPACITY = 1 << 30;
   private static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

   private final long memory;
   private final int pointerCount;
   private final OffHeapMemoryAllocator allocator;

   public MemoryAddressHash(int pointers, OffHeapMemoryAllocator allocator) {
      this.pointerCount = nextPowerOfTwo(pointers);
      long bytes = ((long) pointerCount) << 3;
      this.allocator = allocator;
      memory = allocator.allocate(bytes);
      // Have to clear out bytes to make sure no bad stuff was read in
      UNSAFE.setMemory(memory, bytes, (byte) 0);
   }

   private long findOffset(Object instance) {
      int h = spread(instance.hashCode());
      int pointerMask = pointerCount - 1;
      return h & pointerMask;
   }

   public void putMemoryAddress(Object instance, long address) {
      long offset = findOffset(instance);
      MEMORY.putLong(memory, offset << 3, address);
   }

   public long getMemoryAddress(Object instance) {
      return MEMORY.getLong(memory, findOffset(instance) << 3);
   }

   public long getMemoryAddressOffset(int offset) {
      return MEMORY.getLong(memory,((long) offset) << 3);
   }

   public void deallocate() {
      allocator.deallocate(memory, pointerCount << 3);
   }

   /**
    * Returns a stream of longs that are all of the various memory locations
    * @return stream of the various memory locations
    */
   public LongStream toStream() {
      return LongStream.iterate(memory, l -> l + 8)
            .limit(pointerCount)
            .map(UNSAFE::getLong)
            .filter(l -> l != 0);
   }

   /**
    * Same as {@link MemoryAddressHash#toStream()} except that the memory addresses are also cleared out (set to 0)
    * @return stream with the valid memory pointers to stored values
    */
   public LongStream toStreamRemoved() {
      return LongStream.iterate(memory, l -> l + 8)
            .limit(pointerCount)
            .map(l -> UNSAFE.getAndSetLong(null, l, 0))
            .filter(l -> l != 0);
   }

   private static final int nextPowerOfTwo(int c) {
      int n = c - 1;
      n |= n >>> 1;
      n |= n >>> 2;
      n |= n >>> 4;
      n |= n >>> 8;
      n |= n >>> 16;
      return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
   }

   static final int spread(int h) {
      return (h ^ (h >>> 16)) & HASH_BITS;
   }
}
