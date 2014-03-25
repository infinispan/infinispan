package org.infinispan.context.impl;

import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import java.util.Collection;

/**
 * Invocation context to be used for locally originated transactions.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 4.0
 */
public class LocalTxInvocationContext extends AbstractTxInvocationContext<LocalTransaction> {

   public LocalTxInvocationContext(LocalTransaction localTransaction) {
      super(localTransaction);
   }

   @Override
   public final boolean isTransactionValid() {
      Transaction t = getTransaction();
      int status = -1;
      if (t != null) {
         try {
            status = t.getStatus();
         } catch (SystemException e) {
            // no op
         }
      }
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   @Override
   public final boolean isImplicitTransaction() {
      return getCacheTransaction().isImplicitTransaction();
   }

   @Override
   public final boolean isOriginLocal() {
      return true;
   }

   @Override
   public final boolean hasLockedKey(Object key) {
      return getCacheTransaction().ownsLock(key);
   }

   public final void remoteLocksAcquired(Collection<Address> nodes) {
      getCacheTransaction().locksAcquired(nodes);
   }

   public final Collection<Address> getRemoteLocksAcquired() {
      return getCacheTransaction().getRemoteLocksAcquired();
   }

   @Override
   public final Transaction getTransaction() {
      return getCacheTransaction().getTransaction();
   }
}
