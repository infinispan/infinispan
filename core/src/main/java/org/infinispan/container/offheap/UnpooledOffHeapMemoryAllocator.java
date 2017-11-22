package org.infinispan.container.offheap;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongUnaryOperator;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import sun.misc.Unsafe;

/**
 * Memory allocator that just allocates memory directly using {@link Unsafe}.
 * @author wburns
 * @since 9.0
 */
public class UnpooledOffHeapMemoryAllocator implements OffHeapMemoryAllocator {
   private static final Log log = LogFactory.getLog(UnpooledOffHeapMemoryAllocator.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final OffHeapMemory MEMORY = OffHeapMemory.INSTANCE;
   private final LongAdder amountAllocated = new LongAdder();
   private LongUnaryOperator sizeCalculator;

   @Inject
   public void inject(OffHeapEntryFactory offHeapEntryFactory) {
      sizeCalculator = offHeapEntryFactory::getSize;
   }

   @Override
   public long allocate(long memoryLength) {
      long estimatedMemoryLength = estimateSizeOverhead(memoryLength);
      long memoryLocation = MEMORY.allocate(memoryLength);
      amountAllocated.add(estimatedMemoryLength);
      if (trace) {
         log.tracef("Allocated off heap memory at 0x%016x with %d bytes. Total size: %d", memoryLocation,
               estimatedMemoryLength, amountAllocated.sum());
      }
      return memoryLocation;
   }

   @Override
   public void deallocate(long memoryAddress) {
      // Size calculator already takes care of size estimate
      innerDeallocate(memoryAddress, sizeCalculator.applyAsLong(memoryAddress));
   }

   @Override
   public void deallocate(long memoryAddress, long size) {
      long estimatedMemoryLength = estimateSizeOverhead(size);
      innerDeallocate(memoryAddress, estimatedMemoryLength);
   }

   private void innerDeallocate(long memoryAddress, long estimatedSize) {
      amountAllocated.add(- estimatedSize);
      if (trace) {
         log.tracef("Deallocating off heap memory at 0x%016x with %d bytes. Total size: %d", memoryAddress,
               estimatedSize, amountAllocated.sum());
      }
      MEMORY.free(memoryAddress);
   }

   @Override
   public long getAllocatedAmount() {
      return amountAllocated.sum();
   }

   /**
    * Tries to estimate overhead of the allocation by first adding 8 to account for underlying allocator housekeeping
    * and then rounds up to nearest power of 16 to account for 16 byte alignment.
    * @param size the desired size of the allocation
    * @return the resulting size taking into account various overheads
    */
   public static long estimateSizeOverhead(long size) {
      // We take 8 and add the number provided and then round up to 16 (& operator has higher precedence than +)
      return (size + 8 + 15) & ~0x15;
   }
}
