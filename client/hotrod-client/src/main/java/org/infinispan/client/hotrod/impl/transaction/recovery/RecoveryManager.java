package org.infinispan.client.hotrod.impl.transaction.recovery;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import javax.transaction.xa.Xid;

import org.infinispan.commons.util.concurrent.ConcurrentHashSet;

/**
 * It keeps the local in-doubt transactions.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
//TODO merge with org.infinispan.client.hotrod.impl.transaction.XaModeTransactionTable ?
public class RecoveryManager {

   private final Collection<Xid> preparedTransactions = new ConcurrentHashSet<>();

   public void addTransaction(Xid xid) {
      preparedTransactions.add(xid);
   }

   public void forgetTransaction(Xid xid) {
      preparedTransactions.remove(xid);
   }

   public RecoveryIterator startScan(CompletableFuture<Collection<Xid>> requestFuture) {
      return new RecoveryIterator(preparedTransactions, requestFuture);
   }
}
