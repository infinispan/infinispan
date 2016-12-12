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
   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;
   private static final Log log = LogFactory.getLog(UnpooledOffHeapMemoryAllocator.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final AtomicLong amountAllocated = new AtomicLong();
   private LongUnaryOperator sizeCalculator;

   @Inject
   public void inject(OffHeapEntryFactory offHeapEntryFactory) {
      sizeCalculator = offHeapEntryFactory::determineSize;
   }

   @Override
   public long allocate(long memoryLength) {
      long memoryLocation = UNSAFE.allocateMemory(memoryLength);
      long currentSize = amountAllocated.addAndGet(memoryLength);
      if (trace) {
         log.tracef("Allocated off heap memory at %d with %d bytes.  Total size: %d", memoryLocation, memoryLength,
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
         log.tracef("Deallocating off heap memory at %d with %d bytes.  Total size: %d", memoryAddress, size,
               currentSize);
      }
      UNSAFE.freeMemory(memoryAddress);
   }

   @Override
   public long getAllocatedAmount() {
      return amountAllocated.get();
   }
}
