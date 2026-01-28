package org.infinispan.container.offheap;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Utility method for inserting and retrieving values from to/from a byte[]
 *
 * @author wburns
 * @since 9.0
 */
public class Bits {
   private static final VarHandle INT_VH = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
   private static final VarHandle LONG_VH = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

   static int getInt(byte[] b, int off) {
      return (int) INT_VH.get(b, off);
   }

   static long getLong(byte[] b, int off) {
      return (long) LONG_VH.get(b, off);
   }

   static void putInt(byte[] b, int off, int val) {
      INT_VH.set(b, off, val);
   }

   static void putLong(byte[] b, int off, long val) {
      LONG_VH.set(b, off, val);
   }
}
