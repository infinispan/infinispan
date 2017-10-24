package org.infinispan.container.offheap;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import sun.misc.Unsafe;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple wrapper around Unsafe to provide for trace messages for method calls.
 * @author wburns
 * @since 9.0
 */
class OffHeapMemory {
   protected static final Log log = LogFactory.getLog(OffHeapMemory.class);
   protected static final boolean trace = log.isTraceEnabled();

   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   static final OffHeapMemory INSTANCE = new OffHeapMemory();
   private static final int BYTE_ARRAY_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;

   private static ConcurrentHashMap<Long, Long> allocatedBlocks = new ConcurrentHashMap<>();

   private OffHeapMemory() { }

   byte getByte(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 4);
      byte value = UNSAFE.getByte(srcAddress + offset);
      if (trace) {
         log.tracef("Read byte value 0x%02x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   void putByte(long destAddress, long offset, byte value) {
      checkAddress(destAddress, offset + 4);
      if (trace) {
         log.tracef("Wrote byte value 0x%02x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putByte(destAddress + offset, value);
   }

   int getInt(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 4);
      int value = UNSAFE.getInt(srcAddress + offset);
      if (trace) {
         log.tracef("Read int value 0x%08x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   void putInt(long destAddress, long offset, int value) {
      checkAddress(destAddress, offset + 4);
      if (trace) {
         log.tracef("Wrote int value 0x%08x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putInt(destAddress + offset, value);
   }

   long getLong(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 8);
      long value = UNSAFE.getLong(srcAddress + offset);
      if (trace) {
         log.tracef("Read long value 0x%016x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   void putLong(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      if (trace) {
         log.tracef("Wrote long value 0x%016x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putLong(destAddress + offset, value);
   }

   void getBytes(long srcAddress, long srcOffset, byte[] destArray, long destOffset, long length) {
      checkAddress(srcAddress, srcOffset + length);
      if (trace) {
         log.tracef("Read %d bytes from address 0x%016x+%d into array %s+%d", length, srcAddress, srcOffset, destArray, destOffset);
      }
      UNSAFE.copyMemory(null, srcAddress + srcOffset, destArray, BYTE_ARRAY_BASE_OFFSET + destOffset, length);
   }

   void putBytes(byte[] srcArray, long srcOffset, long destAddress, long destOffset, long length) {
      checkAddress(destAddress, destOffset + length);
      if (trace) {
         log.tracef("Wrote %d bytes from array %s+%d to address 0x%016x+%d", length, srcArray, srcOffset, destAddress, destOffset);
      }
      UNSAFE.copyMemory(srcArray, BYTE_ARRAY_BASE_OFFSET + srcOffset, null, destAddress + destOffset, length);
   }

   /**
    * @deprecated Only use for debugging
    */
   private static byte[] getBytes(long srcAddress, long srcOffset, int length) {
      checkAddress(srcAddress, srcOffset + length);
      byte[] bytes = new byte[length];
      UNSAFE.copyMemory(null, srcAddress + srcOffset, bytes, BYTE_ARRAY_BASE_OFFSET, length);
      return bytes;
   }

   private static void checkAddress(long address, long offset) {
      if (!trace)
         return;

      Long blockSize = allocatedBlocks.get(address);
      if (blockSize == null || blockSize < offset) {
         throw new IllegalArgumentException(String.format("Trying to access address 0x%016x+%d, but blockSize was %d",
               address, offset, blockSize));
      }
   }

   long allocate(long size) {
      long address = UNSAFE.allocateMemory(size);
      if (trace) {
         Long prev = allocatedBlocks.put(address, size);
         if (prev != null) {
            throw new IllegalArgumentException();
         }
      }
      return address;
   }

   void free(long address) {
      Long prev = allocatedBlocks.remove(address);
      if (trace) {
         if (prev == null) {
            throw new IllegalArgumentException();
         }
      }
      UNSAFE.freeMemory(address);
   }
}
