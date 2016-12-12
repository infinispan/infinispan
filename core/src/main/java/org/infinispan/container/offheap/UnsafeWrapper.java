package org.infinispan.container.offheap;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import sun.misc.Unsafe;

/**
 * Simple wrapper around Unsafe to provide for trace messages for method calls.
 * @author wburns
 * @since 9.0
 */
public class UnsafeWrapper {
   protected static final Log log = LogFactory.getLog(UnsafeWrapper.class);
   protected static final boolean trace = log.isTraceEnabled();

   protected static final Unsafe UNSAFE = UnsafeHolder.UNSAFE;

   static final UnsafeWrapper INSTANCE = new UnsafeWrapper();

   private UnsafeWrapper() { }

   public void putLong(long var1, long var3) {
      if (trace) {
         log.tracef("Wrote long value %d to address %d", var3, var1);
      }
      UNSAFE.putLong(var1, var3);
   }

   public void putInt(long var1, int var3) {
      if (trace) {
         log.tracef("Wrote int value %d to address %d", var3, var1);
      }
      UNSAFE.putInt(var1, var3);
   }

   public long getLong(long var1) {
      long var3 = UNSAFE.getLong(var1);
      if (trace) {
         log.tracef("Retrieved long value %d from address %d", var3, var1);
      }
      return var3;
   }

   public int getInt(long var1) {
      int var3 = UNSAFE.getInt(var1);
      if (trace) {
         log.tracef("Retrieved int value %d from address %d", var3, var1);
      }
      return var3;
   }

   public int arrayBaseOffset(Class<?> var1) {
      return UNSAFE.arrayBaseOffset(var1);
   }

   public void copyMemory(Object var1, long var2, Object var4, long var5, long var7) {
      if (trace) {
         log.tracef("Copying memory of object %s offset by %d to %s offset by %d with a total of %d bytes", var1, var2, var4, var5, var7);
      }
      UNSAFE.copyMemory(var1, var2, var4, var5, var7);
   }
}
