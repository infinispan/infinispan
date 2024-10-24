package org.infinispan.container.offheap;

import java.util.stream.LongStream;

import org.infinispan.commons.spi.OffHeapMemory;
import org.infinispan.commons.util.Util;

/**
 * @author wburns
 * @since 9.0
 */
public class MemoryAddressHash {
   private static final OffHeapMemory MEMORY = org.infinispan.commons.jdkspecific.OffHeapMemory.getInstance();

   private final long memory;
   private final int pointerCount;
   private final OffHeapMemoryAllocator allocator;

   public MemoryAddressHash(int pointers, OffHeapMemoryAllocator allocator) {
      this.pointerCount = Util.findNextHighestPowerOfTwo(pointers);
      long bytes = ((long) pointerCount) << 3;
      this.allocator = allocator;
      memory = allocator.allocate(bytes);
      // Have to clear out bytes to make sure no bad stuff was read in
      MEMORY.setMemory(memory, bytes, (byte) 0);
   }

   public void putMemoryAddressOffset(int offset, long address) {
      MEMORY.putLong(memory, (long) offset << 3, address);
   }

   public long getMemoryAddressOffset(int offset) {
      return MEMORY.getLong(memory,((long) offset) << 3);
   }

   public long getMemoryAddressOffsetNoTraceIfAbsent(int offset) {
      return MEMORY.getLongNoTraceIfAbsent(memory,((long) offset) << 3);
   }

   public void deallocate() {
      allocator.deallocate(memory, (long) pointerCount << 3);
   }

   /**
    * Returns a stream of longs that are all of the various memory locations
    * @return stream of the various memory locations
    */
   public LongStream toStream() {
      return LongStream.iterate(0, l -> l + 8)
            .limit(pointerCount)
            .map(l -> MEMORY.getLong(memory, l))
            .filter(l -> l != 0);
   }

   /**
    * Removes all the address lookups by setting them to 0. This method returns a LongStream that contains all of
    * valid (non zero) addresses that were present during this operation.
    * @return stream with the valid memory pointers to stored values
    */
   public LongStream removeAll() {
      return LongStream.iterate(0, l -> l + 8)
            .limit(pointerCount)
            .map(l -> MEMORY.getAndSetLongNoTraceIfAbsent(memory, l, 0))
            .filter(l -> l != 0);
   }

   /**
    * Removes all the address lookups by setting them to 0 within the given offset, limiting the removal to only
    * a specific count of addresses. This method returns a LongStream that contains all of
    * valid (non zero) addresses that were present during this operation.
    * @param offset offset into the block
    * @param count how many pointers to look at
    * @return stream with the valid memory pointers to stored values
    */
   public LongStream removeAll(int offset, int count) {
      return LongStream.iterate(((long) offset) << 3, l -> l + 8)
            .limit(count)
            .map(l -> MEMORY.getAndSetLongNoTraceIfAbsent(memory, l, 0))
            .filter(l -> l != 0);
   }

   public int getPointerCount() {
      return pointerCount;
   }
}
