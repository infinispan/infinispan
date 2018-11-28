package org.infinispan.transaction.tm;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.infinispan.commons.tx.XidImpl;

/**
 * Implementation of {@link Xid} used by {@link EmbeddedTransactionManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class EmbeddedXid extends XidImpl {

   //format can be anything except:
   //-1: means a null Xid
   // 0: means OSI CCR format
   //I would like ot use 0x4953504E (ISPN in hex) for the format, but keep it to 1 to be consistent with DummyXid.
   private static final int FORMAT = 1;
   private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong(1);

   public EmbeddedXid(UUID transactionManagerId) {
      super(FORMAT, create(transactionManagerId, GLOBAL_ID_GENERATOR));

   }

   private static void longToBytes(long val, byte[] array, int offset) {
      for (int i = 7; i > 0; i--) {
         array[offset + i] = (byte) val;
         val >>>= 8;
      }
      array[offset] = (byte) val;
   }

   private static byte[] create(UUID transactionManagerId, AtomicLong generator) {
      byte[] field = new byte[24]; //size of 3 longs
      longToBytes(transactionManagerId.getLeastSignificantBits(), field, 0);
      longToBytes(transactionManagerId.getMostSignificantBits(), field, 8);
      longToBytes(generator.incrementAndGet(), field, 16);
      return field;
   }
}
