package org.infinispan.client.hotrod.impl.transaction.recovery;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.Xid;

/**
 * It keeps the local in-doubt transactions.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
//TODO merge with org.infinispan.client.hotrod.impl.transaction.XaModeTransactionTable ?
public class RecoveryManager {

   private final Collection<Xid> preparedTransactions = ConcurrentHashMap.newKeySet();

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
