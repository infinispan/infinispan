package org.infinispan.transaction.xa;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.CacheRpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.Map;

/**
 * Repository for {@link org.infinispan.transaction.xa.RemoteTransaction} and {@link
 * org.infinispan.transaction.xa.TransactionXaAdapter}s (locally originated trasactions).
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionTable {

   private static Log log = LogFactory.getLog(TransactionTable.class);
   private static boolean trace = log.isTraceEnabled();

   private Map<Transaction, TransactionXaAdapter> localTransactions = new HashMap<Transaction, TransactionXaAdapter>();

   private Map<GlobalTransaction, RemoteTransaction> remoteTransactions = new HashMap<GlobalTransaction, RemoteTransaction>();

   private CommandsFactory commandsFactory;
   private Configuration configuration;
   private InvocationContextContainer icc;
   private InterceptorChain invoker;
   private CacheNotifier notifier;
   private CacheRpcManager rpcManager;


   @Inject
   public void initialize(CommandsFactory commandsFactory, CacheRpcManager rpcManager, Configuration configuration,
                          InvocationContextContainer icc, InterceptorChain invoker, CacheNotifier notifier) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.icc = icc;
      this.invoker = invoker;
      this.notifier = notifier;
   }


   /**
    * Returns the {@link org.infinispan.transaction.xa.RemoteTransaction} associated with the supplied transaction id.
    * Returns null if no such association exists.
    */
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      return remoteTransactions.get(txId);
   }

   /**
    * Creates and register a {@link org.infinispan.transaction.xa.RemoteTransaction} based on the supplied params.
    * Returns the created transaction.
    * @throws IllegalStateException if an attempt to create a {@link org.infinispan.transaction.xa.RemoteTransaction}
    * for an already registered id is made.
    */
   public RemoteTransaction createRemoteTransaction(GlobalTransaction globalTx, WriteCommand[] modifications) {
      RemoteTransaction remoteTransaction = new RemoteTransaction(modifications, globalTx);
      RemoteTransaction transaction = remoteTransactions.put(globalTx, remoteTransaction);
      if (transaction != null) {
         String message = "A remote transaction with the given id was already registred!!!";
         log.error(message);
         throw new IllegalStateException(message);
      }
      if (trace) {
         log.trace("Created and regostered tremote transaction " + remoteTransaction);
      }
      return remoteTransaction;
   }

   /**
    * Returns the {@link org.infinispan.transaction.xa.TransactionXaAdapter} corresponding to the supplied transaction.
    * If none exists, will be created first.
    */
   public TransactionXaAdapter getOrCreateXaAdapter(Transaction transaction, InvocationContext ctx) {
      TransactionXaAdapter current = localTransactions.get(transaction);
      if (current == null) {
         Address localAddress = rpcManager != null ? rpcManager.getLocalAddress() : null;
         GlobalTransaction tx = localAddress == null ? new GlobalTransaction(false) : new GlobalTransaction(localAddress, false);
         current = new TransactionXaAdapter(tx, icc, invoker, commandsFactory, configuration, this, transaction);
         localTransactions.put(transaction, current);
         try {
            transaction.enlistResource(current);
         } catch (Exception e) {
            log.error("Failed to emlist TransactionXaAdapter to transaction");
            throw new CacheException(e);
         }
         notifier.notifyTransactionRegistered(tx, ctx);
      }
      return current;
   }

   /**
    * Removes the {@link org.infinispan.transaction.xa.TransactionXaAdapter} coresponding to the given tx. Returns true
    * if such an tx exists.
    */
   public boolean removeLocalTransaction(Transaction tx) {
      return localTransactions.remove(tx) != null;
   }

   /**
    * Removes the {@link org.infinispan.transaction.xa.RemoteTransaction} coresponding to the given tx. Returns true 
    * if such an tx exists.
    */
   public boolean removeRemoteTransaction(GlobalTransaction txId) {
      return remoteTransactions.remove(txId) != null;
   }

   public int getRemoteTxCount() {
      return remoteTransactions.size();
   }

   public int getLocalTxCount() {
      return localTransactions.size();
   }

   public TransactionXaAdapter getXaCacheAdapter(Transaction tx) {
      return localTransactions.get(tx);
   }
}
