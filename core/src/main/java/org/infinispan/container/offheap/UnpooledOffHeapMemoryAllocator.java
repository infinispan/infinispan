package org.infinispan.container.offheap;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

import org.infinispan.factories.annotations.Inject;

import sun.misc.Unsafe;

/**
 * Memory allocator that just allocates memory directly using {@link Unsafe}.
 * @author wburns
 * @since 9.0
 */
public class UnpooledOffHeapMemoryAllocator implements OffHeapMemoryAllocator {
   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;
   private final AtomicLong amountAllocated = new AtomicLong();
   private LongUnaryOperator sizeCalculator;

   @Inject
   public void inject(OffHeapEntryFactory offHeapEntryFactory) {
      sizeCalculator = offHeapEntryFactory::determineSize;
   }

   @Override
   public long allocate(long memoryLength) {
      long memoryLocation = UNSAFE.allocateMemory(memoryLength);
      amountAllocated.addAndGet(memoryLength);
      return memoryLocation;
   }

   @Override
   public void deallocate(long memoryAddress) {
      amountAllocated.addAndGet(- sizeCalculator.applyAsLong(memoryAddress));
      UNSAFE.freeMemory(memoryAddress);
   }

   @Override
   public long getAllocatedAmount() {
      return amountAllocated.get();
   }
}
