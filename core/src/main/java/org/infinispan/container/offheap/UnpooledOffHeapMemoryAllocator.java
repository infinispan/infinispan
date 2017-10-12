package org.infinispan.container.offheap;

import java.util.concurrent.atomic.AtomicLong;
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
   private final AtomicLong amountAllocated = new AtomicLong();
   private LongUnaryOperator sizeCalculator;

   @Inject
   public void inject(OffHeapEntryFactory offHeapEntryFactory) {
      sizeCalculator = offHeapEntryFactory::getSize;
   }

   @Override
   public long allocate(long memoryLength) {
      long memoryLocation = OffHeapMemory.allocate(memoryLength);
      long currentSize = amountAllocated.addAndGet(memoryLength);
      if (trace) {
         log.tracef("Allocated off heap memory at 0x%016x with %d bytes. Total size: %d", memoryLocation, memoryLength,
               currentSize);
      }
      return memoryLocation;
   }

   @Override
   public void deallocate(long memoryAddress) {
      deallocate(memoryAddress, sizeCalculator.applyAsLong(memoryAddress));
   }

   @Override
   public void deallocate(long memoryAddress, long size) {
      long currentSize = amountAllocated.addAndGet(- size);
      if (trace) {
         log.tracef("Deallocating off heap memory at 0x%016x with %d bytes. Total size: %d", memoryAddress, size,
               currentSize);
      }
      OffHeapMemory.free(memoryAddress);
   }

   @Override
   public long getAllocatedAmount() {
      return amountAllocated.get();
   }
}
