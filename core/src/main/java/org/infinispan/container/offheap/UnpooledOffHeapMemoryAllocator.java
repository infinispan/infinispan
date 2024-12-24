package org.infinispan.container.offheap;

import java.util.concurrent.atomic.LongAdder;

import org.infinispan.commons.spi.OffHeapMemory;
import org.infinispan.metadata.impl.PrivateMetadata;
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
   private static final OffHeapMemory MEMORY = org.infinispan.commons.jdkspecific.OffHeapMemory.getInstance();
   private final LongAdder amountAllocated = new LongAdder();

   @Override
   public long allocate(long memoryLength) {
      long estimatedMemoryLength = estimateSizeOverhead(memoryLength);
      long memoryLocation = MEMORY.allocate(memoryLength);
      amountAllocated.add(estimatedMemoryLength);
      if (log.isTraceEnabled()) {
         log.tracef("Allocated off-heap memory at 0x%016x with %d bytes. Total size: %d", memoryLocation,
               estimatedMemoryLength, amountAllocated.sum());
      }
      return memoryLocation;
   }

   @Override
   public void deallocate(long memoryAddress, long size) {
      long estimatedMemoryLength = estimateSizeOverhead(size);
      innerDeallocate(memoryAddress, estimatedMemoryLength);
   }

   private void innerDeallocate(long memoryAddress, long estimatedSize) {
      amountAllocated.add(- estimatedSize);
      if (log.isTraceEnabled()) {
         log.tracef("Deallocating off-heap memory at 0x%016x with %d bytes. Total size: %d", memoryAddress,
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
    *
    * @param size the desired size of the allocation
    * @return the resulting size taking into account various overheads
    */
   public static long estimateSizeOverhead(long size) {
      // We take 8 and add the number provided and then round up to 16 (& operator has higher precedence than +)
      return (size + 8 + 15) & ~15;
   }

   /**
    * See {@link #offHeapEntrySize(boolean, boolean, int, int, int, int)}
    */
   public static long offHeapEntrySize(boolean evictionEnabled, boolean writeMetadataSize, int keySize, int valueSize) {
      return offHeapEntrySize(evictionEnabled, writeMetadataSize, keySize, valueSize, 0, 0);
   }

   /**
    * It returns the off-heap size of an entry without alignment.
    * <p>
    * If alignment is required, use {@code estimateSizeOverhead(offHeapEntrySize(...))}. See {@link
    * #estimateSizeOverhead(long)},
    *
    * @param evictionEnabled      Set to {@code true} if eviction is enabled.
    * @param writeMetadataSize    Set to {@code true} if the {@link org.infinispan.metadata.Metadata} has versioning or
    *                             it is a custom implementation.
    * @param keySize              The key size.
    * @param valueSize            The value size.
    * @param metadataSize         The {@link org.infinispan.metadata.Metadata} size. If {@code writeMetadataSize} is
    *                             false, this parameter must include the size of mortal/transient entries (2 or 4
    *                             longs).
    * @param internalMetadataSize The {@link PrivateMetadata} size.
    * @return The off-heap entry size without alignment!
    */
   public static long offHeapEntrySize(boolean evictionEnabled, boolean writeMetadataSize, int keySize, int valueSize,
         int metadataSize, int internalMetadataSize) {
      long size = 0;
      if (evictionEnabled) {
         size += 16; // Eviction requires 2 additional pointers at the beginning (8 + 8 bytes)
      }
      if (writeMetadataSize) {
         size += 4; // If has version or it is a custom metadata, we write the metadata size (4 bytes)

      }
      size += 8;  // linked pointer to next address (8 bytes)
      size += OffHeapEntryFactoryImpl.HEADER_LENGTH;
      size += keySize;
      size += valueSize;
      size += metadataSize;
      if (internalMetadataSize > 0) {
         size += 4;
         size += internalMetadataSize;
      }
      return size;
   }
}
