package org.infinispan.commons.jdkspecific;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.infinispan.commons.spi.OffHeapMemory;

/**
 * OffHeapMemory implementation that uses {@link MemorySegment} but in a hacky way to make it work
 * in a way similar to Unsafe. This allocates a new segment per invocation of {@link #allocate(long)}.
 * @author wburns
 * @since 15.1
 */
class UnsafeMemoryAddressOffHeapMemory implements OffHeapMemory {
   private static final Logger log = Logger.getLogger(MethodHandles.lookup().lookupClass().getName());
   private final ConcurrentMap<Long, Long> allocatedBlocks = isFinerEnabled() ? new ConcurrentHashMap<>() : null;

   static final UnsafeMemoryAddressOffHeapMemory INSTANCE = new UnsafeMemoryAddressOffHeapMemory();

   // This is horrible hack to allow us to access all memory addresses like we did with Unsafe
   // Note we track the addresses allocated and verify when TRACE is enabled to make sure
   // callers are using this properly
   static final MemorySegment memorySegment = MemorySegment.ofAddress(0).reinterpret(Long.MAX_VALUE);
   static final MethodHandle allocator;
   static final MethodHandle deallocator;

   public static OffHeapMemory getInstance() {
      return INSTANCE;
   }

   static {
      Linker linker = Linker.nativeLinker();
      String allocatorName = System.getProperty("infinispan.off_heap.allocator", "malloc");
      // Locate the address of malloc()
      var malloc_addr = linker.defaultLookup().find(allocatorName).orElseThrow();

      // Create a downcall handle for malloc()
      allocator = linker.downcallHandle(
            malloc_addr,
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
      );

      String deallocatorName = System.getProperty("infinispan.off_heap.deallocator", "free");

      // Locale the address of free()
      var free_addr = linker.defaultLookup().find(deallocatorName).orElseThrow();

      // Create a downcall handle for free()
      deallocator = linker.downcallHandle(
            free_addr,
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
      );
   }

   private UnsafeMemoryAddressOffHeapMemory() { }

   private static boolean isFinerEnabled() {
      return log.isLoggable(Level.FINER);
   }

   public byte getByte(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 1);
      byte value = memorySegment.get(ValueLayout.JAVA_BYTE, srcAddress + offset);
      if (isFinerEnabled()) {
         log.finer("Read byte value 0x%02x from address 0x%016x+%d".formatted(value, srcAddress, offset));
      }
      return value;
   }

   public void putByte(long destAddress, long offset, byte value) {
      checkAddress(destAddress, offset + 1);
      if (isFinerEnabled()) {
         log.finer("Wrote byte value 0x%02x to address 0x%016x+%d".formatted(value, destAddress, offset));
      }
      memorySegment.set(ValueLayout.JAVA_BYTE, destAddress + offset, value);
   }

   public int getInt(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 4);
      int value = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED,srcAddress + offset);
      if (isFinerEnabled()) {
         log.finer("Read int value 0x%08x from address 0x%016x+%d".formatted(value, srcAddress, offset));
      }
      return value;
   }

   public void putInt(long destAddress, long offset, int value) {
      checkAddress(destAddress, offset + 4);
      if (isFinerEnabled()) {
         log.finer("Wrote int value 0x%08x to address 0x%016x+%d".formatted(value, destAddress, offset));
      }
      memorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, destAddress + offset, value);
   }

   public long getLong(long srcAddress, long offset) {
      return getLong(srcAddress, offset, true);
   }

   public long getAndSetLong(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      if (isFinerEnabled()) {
         log.finer("Get and setting long value 0x%016x to address 0x%016x+%d".formatted(value, destAddress, offset));
      }
      // Note this method can only be invoked with an address that is divisible by 8
      return (long) ValueLayout.JAVA_LONG.varHandle().getAndSet(memorySegment, destAddress + offset, value);
   }

   public long getAndSetLongNoTraceIfAbsent(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      // Note this method can only be invoked with an address that is divisible by 8
      long previous = (long) ValueLayout.JAVA_LONG.varHandle().getAndSet(memorySegment, destAddress + offset, value);
      if (previous != 0) {
         if (isFinerEnabled()) {
            log.finer("Get and set long value 0x%016x to address 0x%016x+%d was 0x%016x".formatted(value, destAddress, offset, previous));
         }
      }
      return previous;
   }

   public long getLongNoTraceIfAbsent(long srcAddress, long offset) {
      return getLong(srcAddress, offset, false);
   }

   private long getLong(long srcAddress, long offset, boolean alwaysTrace) {
      checkAddress(srcAddress, offset + 8);
      long value = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, srcAddress + offset);
      if (isFinerEnabled() && (alwaysTrace || value != 0)) {
         log.finer("Read long value 0x%016x from address 0x%016x+%d".formatted(value, srcAddress, offset));
      }
      return value;
   }

   public void putLong(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      if (isFinerEnabled()) {
         log.finer("Wrote long value 0x%016x to address 0x%016x+%d".formatted(value, destAddress, offset));
      }
      memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, destAddress + offset, value);
   }

   public void getBytes(long srcAddress, long srcOffset, byte[] destArray, long destOffset, long length) {
      checkAddress(srcAddress, srcOffset + length);
      if (isFinerEnabled()) {
         log.finer("Read %d bytes from address 0x%016x+%d into array %s+%d".formatted(length, srcAddress, srcOffset, destArray, destOffset));
      }
      MemorySegment.copy(memorySegment, srcAddress + srcOffset, MemorySegment.ofArray(destArray), destOffset, length);
   }

   public void putBytes(byte[] srcArray, long srcOffset, long destAddress, long destOffset, long length) {
      checkAddress(destAddress, destOffset + length);
      if (isFinerEnabled()) {
         log.finer("Wrote %d bytes from array %s+%d to address 0x%016x+%d".formatted(length, srcArray, srcOffset, destAddress, destOffset));
      }
      MemorySegment.copy(MemorySegment.ofArray(srcArray), srcOffset, memorySegment, destAddress + destOffset, length);
   }

   public void copy(long srcAddress, long srcOffset, long destAddress, long destOffset, long length) {
      checkAddress(srcAddress, srcOffset + length);
      checkAddress(destAddress, destOffset + length);

      if (isFinerEnabled()) {
         log.finer("Copying %d bytes from address 0x%016x+%d to address 0x%016x+%d".formatted(length, srcAddress, srcOffset, destAddress, destOffset));
      }
      MemorySegment.copy(memorySegment, srcAddress + srcOffset, memorySegment, destAddress + destOffset, length);
   }

   private void checkAddress(long address, long offset) {
      if (!isFinerEnabled())
         return;

      Long blockSize = allocatedBlocks.get(address);
      if (blockSize == null || blockSize < offset) {
         throw new IllegalArgumentException(String.format("Trying to access address 0x%016x+%d, but blockSize was %d",
               address, offset, blockSize));
      }
   }

   public long allocate(long size) {
      long address;
      try {
         address = (long) allocator.invokeExact(size);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
      if (isFinerEnabled()) {
         Long prev = allocatedBlocks.put(address, size);
         if (prev != null) {
            throw new IllegalArgumentException();
         }
      }
      return address;
   }

   public void free(long address) {
      if (isFinerEnabled()) {
         Long prev = allocatedBlocks.remove(address);
         if (prev == null) {
            throw new IllegalArgumentException();
         }
      }
      try {
         deallocator.invokeExact(address);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void setMemory(long address, long bytes, byte value) {
      MemorySegment.ofAddress(address)
            .reinterpret(bytes)
            .fill(value);
   }
}
