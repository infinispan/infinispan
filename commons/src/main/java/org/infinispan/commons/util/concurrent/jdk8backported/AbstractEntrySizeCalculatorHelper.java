package org.infinispan.commons.util.concurrent.jdk8backported;

/**
 * Abstract class that provides a method to round up to the nearest value of 8 which is important for most jvm when
 * doing size calculations.  This is due to the fact that most JVMs align to the nearest 8 bytes for addressing
 * purposes.
 * {@inheritDoc}
 */
public abstract class AbstractEntrySizeCalculatorHelper<K, V> implements EntrySizeCalculator<K, V> {
   // This is how large the object header info is
   public static final int OBJECT_SIZE = sun.misc.Unsafe.ADDRESS_SIZE;
   // This is how large an object pointer is - note that each object
   // has to reference its class
   public static final int POINTER_SIZE = sun.misc.Unsafe.ARRAY_OBJECT_INDEX_SCALE;

   public long roundUpToNearest8(long size) {
      return BoundedEquivalentConcurrentHashMapV8.roundUpToNearest8(size);
   }
}
