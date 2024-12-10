package org.infinispan.commons.spi;

public interface OffHeapMemory {
   byte getByte(long srcAddress, long offset);

   void putByte(long destAddress, long offset, byte value);

   int getInt(long srcAddress, long offset);

   void putInt(long destAddress, long offset, int value);

   long getLong(long srcAddress, long offset);

   long getAndSetLong(long destAddress, long offset, long value);

   long getAndSetLongNoTraceIfAbsent(long destAddress, long offset, long value);

   long getLongNoTraceIfAbsent(long srcAddress, long offset);

   void putLong(long destAddress, long offset, long value);

   void getBytes(long srcAddress, long srcOffset, byte[] destArray, long destOffset, long length);

   void putBytes(byte[] srcArray, long srcOffset, long destAddress, long destOffset, long length);

   void copy(long srcAddress, long srcOffset, long destAddress, long destOffset, long length);

   long allocate(long size);

   void free(long address);

   void setMemory(long address, long bytes, byte value);
}
