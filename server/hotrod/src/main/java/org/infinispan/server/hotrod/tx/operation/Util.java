package org.infinispan.server.hotrod.tx.operation;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.functions.SetCompletedTransactionFunction;
import org.infinispan.server.hotrod.tx.table.functions.TxFunction;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ByteString;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class Util {

   public static void rollbackLocalTransaction(AdvancedCache<?, ?> cache, XidImpl xid, long timeout)
         throws HeuristicRollbackException, HeuristicMixedException {
      try {
         completeLocalTransaction(cache, xid, timeout, false);
      } catch (RollbackException e) {
         //ignored since it is always thrown.
      }
   }

   public static void commitLocalTransaction(AdvancedCache<?, ?> cache, XidImpl xid, long timeout)
         throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
      completeLocalTransaction(cache, xid, timeout, true);
   }

   private static void completeLocalTransaction(AdvancedCache<?, ?> cache, XidImpl xid, long timeout, boolean commit)
         throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
      PerCacheTxTable perCacheTxTable = cache.getComponentRegistry().getComponent(PerCacheTxTable.class);
      GlobalTxTable globalTxTable = cache.getComponentRegistry().getGlobalComponentRegistry().getComponent(GlobalTxTable.class);
      try {
         //local transaction
         EmbeddedTransaction tx = perCacheTxTable.getLocalTx(xid);
         tx.runCommit(!commit);
         CacheXid cacheXid = new CacheXid(ByteString.fromString(cache.getName()), xid);
         TxFunction function = new SetCompletedTransactionFunction(commit);
         globalTxTable.update(cacheXid, function, timeout);
      } finally {
         perCacheTxTable.removeLocalTx(xid);
      }
   }

}
