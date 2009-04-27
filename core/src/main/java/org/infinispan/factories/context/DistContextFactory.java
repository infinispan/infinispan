package org.infinispan.factories.context;

import org.infinispan.context.DistTransactionContextImpl;
import org.infinispan.context.TransactionContext;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

/**
 * A context factory specific to DIST contexts
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistContextFactory extends DefaultContextFactory {

   @Override
   public TransactionContext createTransactionContext(Transaction tx) throws SystemException, RollbackException {
      return new DistTransactionContextImpl(tx);
   }
}
