package org.infinispan.commons.jdkspecific;

import java.lang.reflect.Field;

import org.infinispan.commons.CacheException;

import sun.misc.Unsafe;

/**
 * @author wburns
 * @since 9.0
 */
class UnsafeHolder {
   static Unsafe UNSAFE = UnsafeHolder.getUnsafe();

   @SuppressWarnings("restriction")
   private static Unsafe getUnsafe() {
      // attempt to access field Unsafe#theUnsafe
      try {
         final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
         unsafeField.setAccessible(true);
         // the unsafe instance
         return (Unsafe) unsafeField.get(null);
      } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
         throw new CacheException(e);
      }
   }
}
