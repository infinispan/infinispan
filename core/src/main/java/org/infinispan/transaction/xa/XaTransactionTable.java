package org.infinispan.transaction.xa;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.concurrent.ConcurrentMap;

/**
 * {@link TransactionTable} to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class XaTransactionTable extends TransactionTable {

   private static final Log log = LogFactory.getLog(XaTransactionTable.class);

   protected ConcurrentMap<Xid, LocalXaTransaction> xid2LocalTx;
   private RecoveryManager recoveryManager;
   private String cacheName;

   @Inject
   public void init(RecoveryManager recoveryManager, Cache cache) {
      this.recoveryManager = recoveryManager;
      this.cacheName = cache.getName();
   }

   @Start(priority = 9) // Start before cache loader manager
   @SuppressWarnings("unused")
   public void startXidMapping() {
      final int concurrencyLevel = configuration.locking().concurrencyLevel();
      xid2LocalTx = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
   }

   @Override
   public boolean removeLocalTransaction(LocalTransaction localTx) {
      boolean result = false;
      if (localTx.getTransaction() != null) {//this can be null when we force the invocation during recovery, perhaps on a remote node
         result = super.removeLocalTransaction(localTx);
      }
      removeXidTxMapping((LocalXaTransaction) localTx);
      return result;
   }

   private void removeXidTxMapping(LocalXaTransaction localTx) {
      final Xid xid = localTx.getXid();
      xid2LocalTx.remove(xid);
   }

   public LocalXaTransaction getLocalTransaction(Xid xid) {
      return this.xid2LocalTx.get(xid);
   }

   public void addLocalTransactionMapping(LocalXaTransaction localTransaction) {
      if (localTransaction.getXid() == null) throw new IllegalStateException("Initialize xid first!");
      this.xid2LocalTx.put(localTransaction.getXid(), localTransaction);
   }

   @Override
   public void enlist(Transaction transaction, LocalTransaction ltx) {
      LocalXaTransaction localTransaction = (LocalXaTransaction) ltx;
      if (!localTransaction.isEnlisted()) { //make sure that you only enlist it once
         try {
            transaction.enlistResource(new TransactionXaAdapter(
                  localTransaction, this, recoveryManager,
                  txCoordinator, commandsFactory, rpcManager,
                  clusteringLogic, configuration, cacheName, partitionHandlingManager));
         } catch (Exception e) {
            Xid xid = localTransaction.getXid();
            if (xid != null && !localTransaction.getLookedUpEntries().isEmpty()) {
               log.debug("Attempting a rollback to clear stale resources!");
               try {
                  txCoordinator.rollback(localTransaction);
               } catch (XAException xae) {
                  log.debug("Caught exception attempting to clean up " + xid, xae);
               }
            }
            log.failedToEnlistTransactionXaAdapter(e);
            throw new CacheException(e);
         }
      }
   }

   public RecoveryManager getRecoveryManager() {
      return recoveryManager;
   }

   public void setRecoveryManager(RecoveryManager recoveryManager) {
      this.recoveryManager = recoveryManager;
   }

   @Override
   public int getLocalTxCount() {
      return xid2LocalTx.size();
   }
}
