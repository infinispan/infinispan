package org.infinispan.context.impl;

import org.infinispan.transaction.impl.RemoteTransaction;

import javax.transaction.Transaction;

/**
 * Context to be used for transaction that originated remotely.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 4.0
 */
public class RemoteTxInvocationContext extends AbstractTxInvocationContext<RemoteTransaction> {

   public RemoteTxInvocationContext(RemoteTransaction cacheTransaction) {
      super(cacheTransaction);
   }

   @Override
   public final Transaction getTransaction() {
      // this method is only valid for locally originated transactions!
      return null;
   }

   @Override
   public final boolean isTransactionValid() {
      // this is always true since we are governed by the originator's transaction
      return true;
   }

   @Override
   public final boolean isImplicitTransaction() {
      //has no meaning in remote transaction
      return false;
   }

   @Override
   public final boolean isOriginLocal() {
      return false;
   }

   @Override
   public final boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTxInvocationContext)) return false;
      RemoteTxInvocationContext that = (RemoteTxInvocationContext) o;
      return getCacheTransaction().equals(that.getCacheTransaction());
   }

   @Override
   public final int hashCode() {
      return getCacheTransaction().hashCode();
   }
}
