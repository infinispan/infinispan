package org.infinispan.commons.jdkspecific;

import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.spi.OffHeapMemory;

import sun.misc.Unsafe;

/**
 * Simple wrapper around Unsafe to provide for trace messages for method calls.
 * @author wburns
 * @since 9.0
 */
class UnsafeOffHeapMemory implements OffHeapMemory {
   private static final Log log = LogFactory.getLog(UnsafeOffHeapMemory.class);
   private final ConcurrentHashMap<Long, Long> allocatedBlocks = log.isTraceEnabled() ? new ConcurrentHashMap<>() : null;

   private static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   static final OffHeapMemory INSTANCE = new UnsafeOffHeapMemory();
   private static final int BYTE_ARRAY_BASE_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;

   private UnsafeOffHeapMemory() { }

   @Override
   public byte getByte(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 1);
      byte value = UNSAFE.getByte(srcAddress + offset);
      if (log.isTraceEnabled()) {
         log.tracef("Read byte value 0x%02x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   @Override
   public void putByte(long destAddress, long offset, byte value) {
      checkAddress(destAddress, offset + 1);
      if (log.isTraceEnabled()) {
         log.tracef("Wrote byte value 0x%02x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putByte(destAddress + offset, value);
   }

   @Override
   public int getInt(long srcAddress, long offset) {
      checkAddress(srcAddress, offset + 4);
      int value = UNSAFE.getInt(srcAddress + offset);
      if (log.isTraceEnabled()) {
         log.tracef("Read int value 0x%08x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   @Override
   public void putInt(long destAddress, long offset, int value) {
      checkAddress(destAddress, offset + 4);
      if (log.isTraceEnabled()) {
         log.tracef("Wrote int value 0x%08x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putInt(destAddress + offset, value);
   }

   @Override
   public long getLong(long srcAddress, long offset) {
      return getLong(srcAddress, offset, true);
   }

   long getLong(long srcAddress, long offset, boolean alwaysTrace) {
      checkAddress(srcAddress, offset + 8);
      long value = UnsafeOffHeapMemory.UNSAFE.getLong(srcAddress + offset);
      if (UnsafeOffHeapMemory.log.isTraceEnabled() && (alwaysTrace || value != 0)) {
         UnsafeOffHeapMemory.log.tracef("Read long value 0x%016x from address 0x%016x+%d", value, srcAddress, offset);
      }
      return value;
   }

   @Override
   public long getAndSetLong(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      if (log.isTraceEnabled()) {
         log.tracef("Get and setting long value 0x%016x to address 0x%016x+%d", value, destAddress, offset);
      }
      return UNSAFE.getAndSetLong(null, destAddress + offset, value);
   }

   @Override
   public long getAndSetLongNoTraceIfAbsent(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      long previous = UNSAFE.getAndSetLong(null, destAddress + offset, value);
      if (previous != 0) {
         if (log.isTraceEnabled()) {
            log.tracef("Get and set long value 0x%016x to address 0x%016x+%d was 0x%016x", value, destAddress, offset, previous);
         }
      }
      return previous;
   }

   @Override
   public long getLongNoTraceIfAbsent(long srcAddress, long offset) {
      return getLong(srcAddress, offset, false);
   }

   @Override
   public void putLong(long destAddress, long offset, long value) {
      checkAddress(destAddress, offset + 8);
      if (log.isTraceEnabled()) {
         log.tracef("Wrote long value 0x%016x to address 0x%016x+%d", value, destAddress, offset);
      }
      UNSAFE.putLong(destAddress + offset, value);
   }

   @Override
   public void getBytes(long srcAddress, long srcOffset, byte[] destArray, long destOffset, long length) {
      checkAddress(srcAddress, srcOffset + length);
      if (log.isTraceEnabled()) {
         log.tracef("Read %d bytes from address 0x%016x+%d into array %s+%d", length, srcAddress, srcOffset, destArray, destOffset);
      }
      UNSAFE.copyMemory(null, srcAddress + srcOffset, destArray, BYTE_ARRAY_BASE_OFFSET + destOffset, length);
   }

   @Override
   public void putBytes(byte[] srcArray, long srcOffset, long destAddress, long destOffset, long length) {
      checkAddress(destAddress, destOffset + length);
      if (log.isTraceEnabled()) {
         log.tracef("Wrote %d bytes from array %s+%d to address 0x%016x+%d", length, srcArray, srcOffset, destAddress, destOffset);
      }
      UNSAFE.copyMemory(srcArray, BYTE_ARRAY_BASE_OFFSET + srcOffset, null, destAddress + destOffset, length);
   }

   @Override
   public void copy(long srcAddress, long srcOffset, long destAddress, long destOffset, long length) {
      checkAddress(srcAddress, srcOffset + length);
      checkAddress(destAddress, destOffset + length);

      if (log.isTraceEnabled()) {
         log.tracef("Copying %d bytes from address 0x%016x+%d to address 0x%016x+%d", length, srcAddress, srcOffset, destAddress, destOffset);
      }
      UNSAFE.copyMemory(srcAddress + srcOffset, destAddress + destOffset, length);
   }

   private void checkAddress(long address, long offset) {
      if (!log.isTraceEnabled())
         return;

      Long blockSize = allocatedBlocks.get(address);
      if (blockSize == null || blockSize < offset) {
         throw new IllegalArgumentException(String.format("Trying to access address 0x%016x+%d, but blockSize was %d",
               address, offset, blockSize));
      }
   }

   @Override
   public long allocate(long size) {
      long address = UNSAFE.allocateMemory(size);
      if (log.isTraceEnabled()) {
         Long prev = allocatedBlocks.put(address, size);
         if (prev != null) {
            throw new IllegalArgumentException();
         }
      }
      return address;
   }

   @Override
   public void free(long address) {
      if (log.isTraceEnabled()) {
         Long prev = allocatedBlocks.remove(address);
         if (prev == null) {
            throw new IllegalArgumentException();
         }
      }
      UNSAFE.freeMemory(address);
   }

   @Override
   public void setMemory(long address, long bytes, byte value) {
      checkAddress(address, bytes);

      UNSAFE.setMemory(address, bytes, value);
   }
}
