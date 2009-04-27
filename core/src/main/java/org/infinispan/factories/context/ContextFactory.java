package org.infinispan.factories.context;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.TransactionContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

/**
 * A factory for contexts
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface ContextFactory {

   /**
    * @return a new invocation context
    */
   InvocationContext createInvocationContext();


   /**
    * @param tx JTA transaction to associate the new context with
    * @return a new transaction context
    * @throws javax.transaction.RollbackException
    *          in the event of an invalid transaaction
    * @throws javax.transaction.SystemException
    *          in the event of an invalid transaction
    */
   TransactionContext createTransactionContext(Transaction tx) throws SystemException, RollbackException;
}
