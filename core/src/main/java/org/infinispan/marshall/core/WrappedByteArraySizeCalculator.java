package org.infinispan.marshall.core;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.jdk8backported.AbstractEntrySizeCalculatorHelper;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;

/**
 * Size calculator that supports a {@link WrappedByteArray} by adding its size and the underlying byte[].
 * @author wburns
 * @since 9.0
 */
public class WrappedByteArraySizeCalculator<K, V> extends AbstractEntrySizeCalculatorHelper<K, V> {
   private final EntrySizeCalculator chained;

   public WrappedByteArraySizeCalculator(EntrySizeCalculator<?, ?> chained) {
      this.chained = chained;
   }

   @Override
   public long calculateSize(K key, V value) {
      long size = 0;
      Object keyToUse;
      Object valueToUse;
      if (key instanceof WrappedByteArray) {
         keyToUse = ((WrappedByteArray) key).getBytes();
         // WBA object, the class pointer and the pointer to the byte[]
         size += roundUpToNearest8(OBJECT_SIZE + POINTER_SIZE * 2);
      } else {
         keyToUse = key;
      }
      if (value instanceof WrappedByteArray) {
         valueToUse = ((WrappedByteArray) value).getBytes();
         // WBA object, the class pointer and the pointer to the byte[]
         size += roundUpToNearest8(OBJECT_SIZE + POINTER_SIZE * 2);
      } else {
         valueToUse = value;
      }
      return size + chained.calculateSize(keyToUse, valueToUse);
   }
}
