package org.infinispan.context.impl;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;

import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;

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
      super(localTransaction, null);
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

   /**
    * @return {@code true} if there is an {@link IracMetadata} stored for {@code key}.
    */
   public boolean hasIracMetadata(Object key) {
      return getCacheTransaction().hasIracMetadata(key);
   }

   /**
    * Stores the {@link IracMetadata} associated with {@code key}.
    *
    * @param key      The key.
    * @param metadata The {@link CompletionStage} that will be completed with {@link IracMetadata} to associate.
    */
   public void storeIracMetadata(Object key, CompletionStage<IracMetadata> metadata) {
      getCacheTransaction().storeIracMetadata(key, metadata);
   }

   /**
    * @return The {@link IracMetadata} associated with {@code key}.
    */
   public CompletionStage<IracMetadata> getIracMetadata(Object key) {
      return getCacheTransaction().getIracMetadata(key);
   }
}
