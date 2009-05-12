package org.infinispan.transaction.xa;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.container.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.CacheRpcManager;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TxEnlistingManager {
   private TransactionManager tm;
   private InvocationContextContainer icc;
   private InterceptorChain invoker;
   private CacheNotifier notifier;

   private Map<Transaction, TransactionXaAdapter> tx2XaCacheMapping = new ConcurrentHashMap<Transaction, TransactionXaAdapter>();
   private Map<GlobalTransaction, RemoteTxInvocationContext> remoteTxMap;
   private CommandsFactory commandsFactory;
   private CacheRpcManager rpcManager;
   private Configuration configuration;

   @Inject
   public void initialize(CommandsFactory commandsFactory, CacheRpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker,
                          TransactionManager tm, CacheNotifier notifier) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.tm = tm;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
   }

   public TransactionXaAdapter enlist(InvocationContext ctx) throws SystemException, RollbackException {
      Transaction transaction = tm.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      if (!isValid(transaction)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      TransactionXaAdapter current = tx2XaCacheMapping.get(transaction);
      if (current == null) {
         GlobalTransaction tx = rpcManager == null ? new GlobalTransaction(false) : new GlobalTransaction(rpcManager.getLocalAddress(), false);
         current = new TransactionXaAdapter(tx, icc, invoker, commandsFactory, configuration, this, transaction);
         tx2XaCacheMapping.put(transaction, current);
         transaction.enlistResource(current);
         notifier.notifyTransactionRegistered(tx, ctx);
      }
      return current;
   }

   public void delist(Transaction transaction) {
      if (transaction == null) throw new IllegalArgumentException("Null not allowed");
      TransactionXaAdapter xaAdapter = tx2XaCacheMapping.remove(transaction);
      if (xaAdapter == null) {
         throw new IllegalStateException("This method should only be called by a thread that has a tx association.");
      }
   }

   public boolean inTxScope() {
      try {
         if (tm == null) return false;
         Transaction transaction = tm.getTransaction();
         return transaction != null;
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }

   public Transaction getOngoingTx() {
      try {
         return tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }

   public int getNumberOfInitiatedTx() {
      return tx2XaCacheMapping.size();
   }

   private boolean isValid(Transaction tx) {
      return isActive(tx) || isPreparing(tx);
   }

   /**
    * Returns true if transaction is PREPARING, false otherwise
    */
   private boolean isPreparing(Transaction tx) {
      if (tx == null) return false;
      int status;
      try {
         status = tx.getStatus();
         return status == Status.STATUS_PREPARING;
      }
      catch (SystemException e) {
         return false;
      }
   }

   /**
    * Returns true if transaction is ACTIVE, false otherwise
    */
   private boolean isActive(Transaction tx) {
      if (tx == null) return false;
      int status;
      try {
         status = tx.getStatus();
         return status == Status.STATUS_ACTIVE;
      }
      catch (SystemException e) {
         return false;
      }
   }

   public int getActiveLocallyInitiatedTxCount() {
      if (this.tx2XaCacheMapping == null) return 0;
      return tx2XaCacheMapping.size();
   }

   public int getActiveRemotelyInitiatedTxCount() {
      if (this.remoteTxMap == null) return 0;
      return this.remoteTxMap.size();
   }

   public TransactionXaAdapter getXaCache(Transaction tx) {
      return tx2XaCacheMapping.get(tx);   
   }

   public Transaction getRunningTx() {
      try {
         return tm == null ? null : tm.getTransaction();
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }
}
