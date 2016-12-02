package org.infinispan.container.offheap;

/**
 * Allows for allocation of memory outside of the heap as well additional functionality surrounding it if
 * necessary.
 * @author wburns
 * @since 9.0
 */
public interface OffHeapMemoryAllocator {
   /**
    * Allocates a new chunk of memory sized to the given length.
    * @param memoryLength the size of memory to allocate
    * @return the memory address where the memory resides
    */
   long allocate(long memoryLength);

   /**
    * Deallocates the memory allocated at the given address
    * @param memoryAddress the address returned from the allocator to deallocate
    */
   void deallocate(long memoryAddress);

   long getAllocatedAmount();
}
