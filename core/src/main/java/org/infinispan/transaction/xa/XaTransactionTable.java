package org.infinispan.transaction.xa;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link TransactionTable} to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class XaTransactionTable extends TransactionTable {

   private static final Log log = LogFactory.getLog(XaTransactionTable.class);

   protected final ConcurrentMap<Xid, LocalXaTransaction> xid2LocalTx = new ConcurrentHashMap<Xid, LocalXaTransaction>();
   private RecoveryManager recoveryManager;

   @Inject
   public void init(RecoveryManager recoveryManager) {
      this.recoveryManager = recoveryManager;
   }

   @Override
   public boolean removeLocalTransaction(LocalTransaction localTx) {
      boolean result = super.removeLocalTransaction(localTx);
      LocalXaTransaction xaLocalTransaction = (LocalXaTransaction) localTx;
      xid2LocalTx.remove(xaLocalTransaction.getXid());
      return result;
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
            transaction.enlistResource(new TransactionXaAdapter(localTransaction, this, configuration, icc, recoveryManager, txCoordinator));
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
            log.error("Failed to enlist TransactionXaAdapter to transaction", e);
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
}
