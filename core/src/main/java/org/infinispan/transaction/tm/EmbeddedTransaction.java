package org.infinispan.transaction.tm;


import static org.infinispan.commons.util.Util.longToBytes;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.XidImpl;

/**
 * A {@link Transaction} implementation used by {@link EmbeddedBaseTransactionManager}.
 * <p>
 * See {@link EmbeddedBaseTransactionManager} for more details.
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @see EmbeddedBaseTransactionManager
 * @since 9.0
 */
public final class EmbeddedTransaction extends TransactionImpl {

   //format can be anything except:
   //-1: means a null Xid
   // 0: means OSI CCR format
   //I would like ot use 0x4953504E (ISPN in hex) for the format, but keep it to 1 to be consistent with DummyXid.
   private static final int FORMAT = 1;
   private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong(1);
   private static final AtomicLong BRANCH_QUALIFIER_GENERATOR = new AtomicLong(1);

   public EmbeddedTransaction(EmbeddedBaseTransactionManager tm) {
      super();
      setXid(createXid(tm.getTransactionManagerId()));
   }

   public XAResource firstEnlistedResource() {
      return getEnlistedResources().iterator().next();
   }

   public static XidImpl createXid(UUID transactionManagerId) {
      return XidImpl.create(FORMAT, create(transactionManagerId, GLOBAL_ID_GENERATOR),
            create(transactionManagerId, BRANCH_QUALIFIER_GENERATOR));
   }

   private static byte[] create(UUID transactionManagerId, AtomicLong generator) {
      byte[] field = new byte[24]; //size of 3 longs
      longToBytes(transactionManagerId.getLeastSignificantBits(), field, 0);
      longToBytes(transactionManagerId.getMostSignificantBits(), field, 8);
      longToBytes(generator.incrementAndGet(), field, 16);
      return field;
   }
}
