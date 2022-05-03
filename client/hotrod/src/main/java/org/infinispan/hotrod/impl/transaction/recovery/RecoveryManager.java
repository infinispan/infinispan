package org.infinispan.hotrod.impl.transaction.recovery;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.Xid;

/**
 * It keeps the local in-doubt transactions.
 *
 * @since 14.0
 */
//TODO merge with org.infinispan.hotrod.impl.transaction.XaModeTransactionTable ?
public class RecoveryManager {

   private final Collection<Xid> preparedTransactions = ConcurrentHashMap.newKeySet();

   public void addTransaction(Xid xid) {
      preparedTransactions.add(xid);
   }

   public void forgetTransaction(Xid xid) {
      preparedTransactions.remove(xid);
   }

   public RecoveryIterator startScan(CompletionStage<Collection<Xid>> requestFuture) {
      return new RecoveryIterator(preparedTransactions, requestFuture);
   }
}
