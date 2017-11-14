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
      long roundedMemoryLength = roundUpTo8(memoryLength);
      long memoryLocation = MEMORY.allocate(memoryLength);
      amountAllocated.add(roundedMemoryLength);
      if (trace) {
         log.tracef("Allocated off heap memory at 0x%016x with %d bytes. Total size: %d", memoryLocation,
               roundedMemoryLength, amountAllocated.sum());
      }
      return memoryLocation;
   }

   @Override
   public void deallocate(long memoryAddress) {
      deallocate(memoryAddress, sizeCalculator.applyAsLong(memoryAddress));
   }

   @Override
   public void deallocate(long memoryAddress, long size) {
      long roundedMemoryLength = roundUpTo8(size);
      amountAllocated.add(- roundedMemoryLength);
      if (trace) {
         log.tracef("Deallocating off heap memory at 0x%016x with %d bytes. Total size: %d", memoryAddress,
               roundedMemoryLength, amountAllocated.sum());
      }
      MEMORY.free(memoryAddress);
   }

   @Override
   public long getAllocatedAmount() {
      return amountAllocated.sum();
   }

   /**
    * Round up the size to a multiple of 8 to account for most memory allocators aligning allocations to a multiple of 8
    * @param size the size to align to 8
    * @return the resulting aligned value
    */
   public static long roundUpTo8(long size) {
      return (size + 7) & ~0x7;
   }
}
