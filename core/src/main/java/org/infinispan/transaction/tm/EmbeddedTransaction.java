package org.infinispan.transaction.tm;


import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.infinispan.commons.tx.TransactionImpl;

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

   public EmbeddedTransaction(EmbeddedBaseTransactionManager tm) {
      super();
      setXid(new EmbeddedXid(tm.getTransactionManagerId()));
   }

   public XAResource firstEnlistedResource() {
      return getEnlistedResources().iterator().next();
   }
}
