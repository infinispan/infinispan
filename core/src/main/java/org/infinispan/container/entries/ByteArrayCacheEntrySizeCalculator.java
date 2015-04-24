package org.infinispan.container.entries;

import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.AbstractSizeCalculatorHelper;

/**
 * Entry Size calculator that returns an approximation of how much memory the byte[] for the key and value
 * use.
 * @author wburns
 * @since 8.0
 */
public class ByteArrayCacheEntrySizeCalculator extends AbstractSizeCalculatorHelper<byte[], byte[]> {
   public long calculateSize(byte[] key, byte[] value) {
      long keySize = byteArraySize(key);
      long valueSize = byteArraySize(value);
      return keySize + valueSize;
   }

   private long byteArraySize(byte[] array) {
      // There is an offset for the first entry that is overhead
      long size = ARRAY_BYTE_BASE_OFFSET;
      // Add in how much the offset is for each byte
      size += array.length * ARRAY_BYTE_OFFSET;
      // Arrays are also rounded to nearest size of 8
      return roundUpToNearest8(size);
   }

   // Each array has a minimal overhead before the location of the first element
   private final static int ARRAY_BYTE_BASE_OFFSET = sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
   // The offset between bytes is essentially the size for each
   private final static int ARRAY_BYTE_OFFSET = sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
}
