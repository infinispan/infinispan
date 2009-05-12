package org.infinispan.transaction.xa;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.container.InvocationContextContainer;
import org.infinispan.context.impl.InitiatorTxInvocationContext;
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
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionXaAdapter implements XAResource {

   private static Log log = LogFactory.getLog(TransactionXaAdapter.class);

   private int txTimeout;

   private List<WriteCommand> modifications;
   private BidirectionalMap<Object, CacheEntry> lookedUpEntries;

   private GlobalTransaction transactionIdentifier;
   private InvocationContextContainer icc;
   private InterceptorChain invoker;

   private CommandsFactory commandsFactory;
   private Configuration configuration;

   private TxEnlistingManager txEnlistingManager;
   private Transaction transaction;

   public TransactionXaAdapter(GlobalTransaction transactionIdentifier, InvocationContextContainer icc, InterceptorChain invoker,
                  CommandsFactory commandsFactory, Configuration configuration, TxEnlistingManager txEnlistingManager,
                  Transaction transaction) {
      this.transactionIdentifier = transactionIdentifier;
      this.icc = icc;
      this.invoker = invoker;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.txEnlistingManager = txEnlistingManager;
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
         if (log.isTraceEnabled())
            log.trace("Recieved prepare for tx: " + xid + " . Skipping call as 1PC will be used.");
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandsFactory.buildPrepareCommand(transactionIdentifier, modifications, configuration.isOnePhaseCommit());
      if (log.isTraceEnabled()) log.trace("Sending prepare command through the chain: " + prepareCommand);

      InitiatorTxInvocationContext ctx = icc.getInitiatorTxInvocationContext();
      ctx.setXaCache(this);
      try {
         invoker.invoke(ctx, prepareCommand);
         return XA_OK; //todo validate code here
      } catch (Throwable e) {
         log.error("Error while processing PrepareCommand", e);
         throw new XAException(XAException.XAER_RMERR);//todo validate code here
      }
   }

   public void commit(Xid xid, boolean b) throws XAException {
      if (log.isTraceEnabled()) log.trace("commiting TransactionXaAdapter: " + transactionIdentifier);
      try {
         InitiatorTxInvocationContext ctx = icc.getInitiatorTxInvocationContext();
         ctx.setXaCache(this);
         if (configuration.isOnePhaseCommit()) {
            if (log.isTraceEnabled()) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(transactionIdentifier, modifications, true);
            try {
               invoker.invoke(ctx, command);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(transactionIdentifier);
            try {
               invoker.invoke(ctx, commitCommand);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         txEnlistingManager.delist(transaction);
         this.modifications = null;
      }
   }

   public void rollback(Xid xid) throws XAException {
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(transactionIdentifier);
      InitiatorTxInvocationContext ctx = icc.getInitiatorTxInvocationContext();
      ctx.setXaCache(this);
      try {
         invoker.invoke(ctx, rollbackCommand);
      } catch (Throwable e) {
         log.error("Exception while ", e);
         throw new XAException(XAException.XA_HEURHAZ);
      } finally {
         txEnlistingManager.delist(transaction);
         this.modifications = null;
      }
   }

   public void start(Xid xid, int i) throws XAException {
      if (log.isTraceEnabled()) log.trace("start called");
   }

   public void end(Xid xid, int i) throws XAException {
      if (log.isTraceEnabled()) log.trace("end called");
   }

   public void forget(Xid xid) throws XAException {
      if (log.isTraceEnabled()) log.trace("forget called");
   }

   public int getTransactionTimeout() throws XAException {
      if (log.isTraceEnabled()) log.trace("start called");
      return txTimeout;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException {
      if (!(xaResource instanceof TransactionXaAdapter)) {
         return false;
      }
      TransactionXaAdapter other = (TransactionXaAdapter) xaResource;
      return other.transactionIdentifier.equals(this.transactionIdentifier);
   }

   public Xid[] recover(int i) throws XAException {
      if (log.isTraceEnabled()) log.trace("recover called: " + i);
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

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
   }

   private void initLookedUpEntries() {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
   }

   public GlobalTransaction getTransactionIdentifier() {
      return transactionIdentifier;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public Transaction getTransaction() {
      return transaction;
   }
}
