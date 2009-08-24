package org.infinispan.transaction.xa;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This acts both as an local {@link org.infinispan.transaction.xa.CacheTransaction} and implementor of an {@link
 * javax.transaction.xa.XAResource} that will be called by tx manager on various tx stages.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionXaAdapter implements CacheTransaction, XAResource {

   private static Log log = LogFactory.getLog(TransactionXaAdapter.class);
   private static boolean trace = log.isTraceEnabled();

   private int txTimeout;

   private List<WriteCommand> modifications;
   private BidirectionalMap<Object, CacheEntry> lookedUpEntries;

   private GlobalTransaction globalTx;
   private InvocationContextContainer icc;
   private InterceptorChain invoker;

   private CommandsFactory commandsFactory;
   private Configuration configuration;

   private TransactionTable txTable;
   private Transaction transaction;

   public TransactionXaAdapter(GlobalTransaction globalTx, InvocationContextContainer icc, InterceptorChain invoker,
                               CommandsFactory commandsFactory, Configuration configuration, TransactionTable txTable,
                               Transaction transaction) {
      this.globalTx = globalTx;
      this.icc = icc;
      this.invoker = invoker;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.txTable = txTable;
      this.transaction = transaction;
   }

   public void addModification(WriteCommand mod) {
      if (modifications == null) {
         modifications = new ArrayList<WriteCommand>(8);
      }
      modifications.add(mod);
   }

   public int prepare(Xid xid) throws XAException {
      if (configuration.isOnePhaseCommit()) {
         if (trace)
            log.trace("Recieved prepare for tx: " + xid + " . Skipping call as 1PC will be used.");
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandsFactory.buildPrepareCommand(globalTx, modifications, configuration.isOnePhaseCommit());
      if (trace) log.trace("Sending prepare command through the chain: " + prepareCommand);

      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setXaCache(this);
      try {
         invoker.invoke(ctx, prepareCommand);
         return XA_OK; //todo validate code here
      } catch (Throwable e) {
         log.error("Error while processing PrepareCommand", e);
         throw new XAException(XAException.XAER_RMERR);//todo validate code here
      }
   }

   public void commit(Xid xid, boolean isOnePhase) throws XAException {
      // always call prepare() - even if this is just a 1PC!
      if (isOnePhase) prepare(xid);
      if (trace) log.trace("commiting TransactionXaAdapter: " + globalTx);
      try {
         LocalTxInvocationContext ctx = icc.createTxInvocationContext();
         ctx.setXaCache(this);
         if (configuration.isOnePhaseCommit()) {
            if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(globalTx, modifications, true);
            try {
               invoker.invoke(ctx, command);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(globalTx);
            try {
               invoker.invoke(ctx, commitCommand);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         txTable.removeLocalTransaction(transaction);
         this.modifications = null;
      }
   }

   public void rollback(Xid xid) throws XAException {
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(globalTx);
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setXaCache(this);
      try {
         invoker.invoke(ctx, rollbackCommand);
      } catch (Throwable e) {
         log.error("Exception while rollback", e);
         throw new XAException(XAException.XA_HEURHAZ);
      } finally {
         txTable.removeLocalTransaction(transaction);
         this.modifications = null;
      }
   }

   public void start(Xid xid, int i) throws XAException {
      if (trace) log.trace("start called on tx " + this.globalTx);
   }

   public void end(Xid xid, int i) throws XAException {
      if (trace) log.trace("end called on tx " + this.globalTx);
   }

   public void forget(Xid xid) throws XAException {
      if (trace) log.trace("forget called");
   }

   public int getTransactionTimeout() throws XAException {
      if (trace) log.trace("start called");
      return txTimeout;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException {
      if (!(xaResource instanceof TransactionXaAdapter)) {
         return false;
      }
      TransactionXaAdapter other = (TransactionXaAdapter) xaResource;
      return other.equals(this);
   }

   public Xid[] recover(int i) throws XAException {
      if (trace) log.trace("recover called: " + i);
      return null; //todo validate with javadoc
   }

   public boolean setTransactionTimeout(int i) throws XAException {
      this.txTimeout = i;
      return true; //todo check javadoc
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      initLookedUpEntries();
      lookedUpEntries.putAll(entries);
   }

   public CacheEntry lookupEntry(Object key) {
      if (lookedUpEntries == null) return null;
      return lookedUpEntries.get(key);
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return (BidirectionalMap<Object, CacheEntry>)
            (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      initLookedUpEntries();
      lookedUpEntries.put(key, e);
   }

   private void initLookedUpEntries() {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
   }

   public GlobalTransaction getGlobalTx() {
      return globalTx;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTx;
   }

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransactionXaAdapter)) return false;

      TransactionXaAdapter that = (TransactionXaAdapter) o;

      if (!globalTx.equals(that.globalTx)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return globalTx.hashCode();
   }

   @Override
   public String toString() {
      return "TransactionXaAdapter{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", globalTx=" + globalTx +
            ", transaction=" + transaction +
            ", txTimeout=" + txTimeout +
            '}';
   }
}
