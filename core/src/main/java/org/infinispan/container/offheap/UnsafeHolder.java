package org.infinispan.container.offheap;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

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
      final Object maybeUnsafe = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
               final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
               unsafeField.setAccessible(true);
               // the unsafe instance
               return unsafeField.get(null);
            } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
               return e;
            }
         });
      if (maybeUnsafe instanceof Exception) {
         throw new CacheException((Exception) maybeUnsafe);
      } else {
         return (Unsafe) maybeUnsafe;
      }
   }
}
