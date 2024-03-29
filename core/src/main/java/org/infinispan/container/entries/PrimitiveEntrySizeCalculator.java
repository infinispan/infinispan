package org.infinispan.container.entries;

import java.lang.reflect.Array;

import org.infinispan.commons.util.AbstractEntrySizeCalculatorHelper;

import sun.misc.Unsafe;

/**
 * Entry Size calculator that returns an approximation of how much various primitives, primitive wrappers, Strings, and
 * arrays
 *
 * @author wburns
 * @since 8.0
 */
public class PrimitiveEntrySizeCalculator extends AbstractEntrySizeCalculatorHelper<Object, Object> {
   public long calculateSize(Object key, Object value) {
      return handleObject(key) + handleObject(value);
   }

   protected long handleObject(Object object) {
      Class<?> objClass;
      try {
         objClass = object.getClass();
      } catch (NullPointerException e) {
         throw e;
      }
      if (objClass == String.class) {
         String realString = (String) object;
         // The string is an object and has a reference to its class, int for the hash code and a pointer to the char[]
         long objectSize = roundUpToNearest8(OBJECT_SIZE + POINTER_SIZE + 4 + POINTER_SIZE);
         // We then include the char[] offset and size
         return objectSize + roundUpToNearest8(Unsafe.ARRAY_CHAR_BASE_OFFSET + realString.length() *
               Unsafe.ARRAY_CHAR_INDEX_SCALE);
      } else if (objClass == Long.class) {
         long longValue = ((Long) object).longValue();
         if (longValue >= LongCacheConstraints.MIN_CACHE_VALUE &&
               longValue <= LongCacheConstraints.MAX_CACHE_VALUE) {
            return 0;
         }
         // We add in the size for a long, plus the object reference and the class ref
         return roundUpToNearest8(Unsafe.ARRAY_LONG_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Integer.class) {
         int intValue = ((Integer) object).intValue();
         if (intValue >= IntegerCacheConstraints.MIN_CACHE_VALUE &&
               intValue <= IntegerCacheConstraints.MAX_CACHE_VALUE) {
            return 0;
         }
         // We add in the size for a long, plus the object reference and the class ref
         return roundUpToNearest8(Unsafe.ARRAY_INT_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Short.class) {
         short shortValue = ((Short) object).shortValue();
         if (shortValue >= ShortCacheConstraints.MIN_CACHE_VALUE &&
               shortValue <= ShortCacheConstraints.MAX_CACHE_VALUE) {
            return 0;
         }
         return roundUpToNearest8(Unsafe.ARRAY_SHORT_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Double.class) {
         return roundUpToNearest8(Unsafe.ARRAY_DOUBLE_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Float.class) {
         return roundUpToNearest8(Unsafe.ARRAY_FLOAT_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Boolean.class) {
         // We assume all provided booleans are cached
         return 0;
      } else if (objClass == Character.class) {
         char charValue = ((Character) object).charValue();
         if (charValue >= CharacterCacheConstraints.MIN_CACHE_VALUE &&
               charValue <= CharacterCacheConstraints.MAX_CACHE_VALUE) {
            return 0;
         }
         return roundUpToNearest8(Unsafe.ARRAY_CHAR_INDEX_SCALE + OBJECT_SIZE + POINTER_SIZE);
      } else if (objClass == Byte.class) {
         // All byte values are cached
         return 0;
      } else if (objClass.isArray()) {
         // We assume the array is of a type that supports shallow copy, such as the ones above
         // We don't verify cached values if the array contains Booleans for example
         Unsafe unsafe = getUnsafe();
         Class<?> compClass = objClass.getComponentType();
         int arrayLength = Array.getLength(object);
         // Every array has a base offset which defines how large the array is and other overhead.
         // Then each element in the array is indexed contiguously in memory thus we can simply multiply how
         // many elements are in the array by how much of an offset each element requires.  A normal object for example
         // takes up the standard Object pointer worth of size but primitives take up space equivalent to how many byte
         // they occupy.
         long arraySize = roundUpToNearest8(unsafe.arrayBaseOffset(objClass) + unsafe.arrayIndexScale(objClass) *
               arrayLength);
         // If the component type isn't primitive we have to add in each of the instances
         if (!compClass.isPrimitive()) {
            // TODO: we could assume some values for given primitive wrappers.
            for (int i = 0; i < arrayLength; ++i) {
               arraySize += handleObject(Array.get(object, i));
            }
         }
         return arraySize;
      } else {
         throw new IllegalArgumentException("Size of Class " + objClass +
               " cannot be determined using given entry size calculator :" + getClass());
      }
   }

   static class CharacterCacheConstraints {
      static final short MAX_CACHE_VALUE = 127;
      static final short MIN_CACHE_VALUE = 0;
   }

   static class ShortCacheConstraints {
      static final short MAX_CACHE_VALUE = 127;
      static final short MIN_CACHE_VALUE = -128;
   }

   static class LongCacheConstraints {
      static final long MAX_CACHE_VALUE = 127;
      static final long MIN_CACHE_VALUE = -128;
   }

   static class IntegerCacheConstraints {
      static final int MAX_CACHE_VALUE = calculateMaxIntCache();
      static final int MIN_CACHE_VALUE = -128;

      static int calculateMaxIntCache() {
         //We start from 128 as caching numbers up to 127 is required by the JLS:
         //see 5.1.7 Boxing Conversion
         for (int i = 128; i < Integer.MAX_VALUE; i++) {
            if (Integer.valueOf(i) == Integer.valueOf(i))
               continue;
            else
               return i - 1;
         }
         return Integer.MAX_VALUE;
      }
   }

   /**
    * Returns a sun.misc.Unsafe.  Suitable for use in a 3rd party package. Replace with a simple call to
    * Unsafe.getUnsafe when integrating into a jdk.
    *
    * @return a sun.misc.Unsafe
    */
   static Unsafe getUnsafe() {
      try {
         return Unsafe.getUnsafe();
      } catch (SecurityException tryReflectionInstead) {
         try {
            Class<Unsafe> k = Unsafe.class;
            for (java.lang.reflect.Field f : k.getDeclaredFields()) {
               f.setAccessible(true);
               Object x = f.get(null);
               if (k.isInstance(x))
                  return k.cast(x);
            }
            throw new NoSuchFieldError("the Unsafe");
         } catch (Exception e) {
            throw new RuntimeException("Could not initialize intrinsics", e.getCause());
         }
      }
   }
}
