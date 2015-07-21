package org.infinispan.interceptors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.TransactionAwareEntryIterable;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingEntryCacheSet;
import org.infinispan.stream.impl.interceptor.AbstractDelegatingKeyCacheSet;
import org.infinispan.stream.impl.spliterators.IteratorAsSpliterator;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoverableTransactionIdentifier;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with handling transaction related operations, e.g enlisting cache as an transaction
 * participant, propagating remotely initiated changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.transaction.xa.TransactionXaAdapter
 * @since 4.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor<K, V> extends CommandInterceptor implements JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(TxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private Cache<K, V> cache;
   private RecoveryManager recoveryManager;
   private TransactionTable txTable;
   private PartitionHandlingManager partitionHandlingManager;

   private boolean isTotalOrder;
   private boolean useOnePhaseForAutoCommitTx;
   private boolean useVersioning;
   private boolean statisticsEnabled;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(TransactionTable txTable, Configuration configuration, RpcManager rpcManager,
                    RecoveryManager recoveryManager, CommandsFactory commandsFactory, Cache<K, V> cache,
                    PartitionHandlingManager partitionHandlingManager) {
      this.cacheConfiguration = configuration;
      this.txTable = txTable;
      this.rpcManager = rpcManager;
      this.recoveryManager = recoveryManager;
      this.commandsFactory = commandsFactory;
      this.cache = cache;
      this.partitionHandlingManager = partitionHandlingManager;

      statisticsEnabled = cacheConfiguration.jmxStatistics().enabled();
      isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      useOnePhaseForAutoCommitTx = cacheConfiguration.transaction().use1PcForAutoCommitTransactions();
      useVersioning = Configurations.isVersioningEnabled(configuration);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //if it is remote and 2PC then first log the tx only after replying mods
      if (this.statisticsEnabled) prepares.incrementAndGet();
      if (!ctx.isOriginLocal()) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());
      } else {
         if (ctx.getCacheTransaction().hasModification(ClearCommand.class)) {
            throw new IllegalStateException("No ClearCommand is allowed in Transaction.");
         }
      }
      Object result = invokeNextInterceptorAndVerifyTransaction(ctx, command);
      if (!ctx.isOriginLocal()) {
         if (command.isOnePhaseCommit()) {
            txTable.remoteTransactionCommitted(command.getGlobalTransaction(), true);
         } else {
            txTable.remoteTransactionPrepared(command.getGlobalTransaction());
         }
      }
      return result;
   }

   private Object invokeNextInterceptorAndVerifyTransaction(TxInvocationContext ctx, AbstractTransactionBoundaryCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isOriginLocal()) {
            verifyRemoteTransaction((RemoteTxInvocationContext) ctx, command);
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      // TODO The local origin check is needed for CommitFailsTest, but it doesn't appear correct to roll back an in-doubt tx
      if (!ctx.isOriginLocal()) {
         if (txTable.isTransactionCompleted(gtx)) {
            if (trace) log.tracef("Transaction %s already completed, skipping commit", gtx);
            return null;
         }

         if (!isTotalOrder) {
            replayRemoteTransactionIfNeeded((RemoteTxInvocationContext) ctx, command.getTopologyId());
         }
      }

      if (this.statisticsEnabled) commits.incrementAndGet();
      Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isOriginLocal() || isTotalOrder) {
         txTable.remoteTransactionCommitted(gtx, false);
      }
      return result;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      // The transaction was marked as completed in RollbackCommand.prepare()
      if (!ctx.isOriginLocal() || isTotalOrder) {
         txTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         //for tx that rollback we do not send a TxCompletionNotification, so we should cleanup
         // the recovery info here
         if (recoveryManager != null) {
            GlobalTransaction gtx = command.getGlobalTransaction();
            recoveryManager.removeRecoveryInformation(((RecoverableTransactionIdentifier) gtx).getXid());
         }
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      enlistIfNeeded(ctx);

      if (ctx.isOriginLocal()) {
         command.setGlobalTransaction(ctx.getGlobalTransaction());
      }

      return invokeNextInterceptorAndVerifyTransaction(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public CacheSet<K> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      CacheSet<K> set = (CacheSet<K>) enlistReadAndInvokeNext(ctx, command);
      if (ctx.isInTxScope()) {
         return new AbstractDelegatingKeyCacheSet(getCacheWithFlags(cache, command), set) {
            @Override
            public CloseableIterator<K> iterator() {
               return new TransactionAwareKeyCloseableIterator<>(super.iterator(),
                       (TxInvocationContext<LocalTransaction>) ctx, cache);
            }

            @Override
            public CloseableSpliterator<K> spliterator() {
               Spliterator<K> parentSpliterator = super.spliterator();
               long estimateSize = parentSpliterator.estimateSize() + ctx.getLookedUpEntries().size();
               // This is an overestimate for size if we have looked up entries that don't map to this node
               return new IteratorAsSpliterator.Builder<>(iterator())
                       .setEstimateRemaining(estimateSize < 0L ? Long.MAX_VALUE : estimateSize)
                       .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                       .get();
            }

            @Override
            public int size() {
               long size = stream().count();
               if (size > Integer.MAX_VALUE) {
                  return Integer.MAX_VALUE;
               }
               return (int) size;
            }
         };
      }
      return set;
   }

   @Override
   public CacheSet<CacheEntry<K, V>> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      CacheSet<CacheEntry<K, V>> set = (CacheSet<CacheEntry<K, V>>) enlistReadAndInvokeNext(ctx, command);
      if (ctx.isInTxScope()) {
         return new AbstractDelegatingEntryCacheSet<K, V>(getCacheWithFlags(cache, command), set) {
            @Override
            public CloseableIterator<CacheEntry<K, V>> iterator() {
               return new TransactionAwareEntryCloseableIterator<>(super.iterator(),
                       (TxInvocationContext<LocalTransaction>) ctx, cache);
            }

            @Override
            public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
               Spliterator<CacheEntry<K, V>> parentSpliterator = super.spliterator();
               long estimateSize = parentSpliterator.estimateSize() + ctx.getLookedUpEntries().size();
               // This is an overestimate for size if we have looked up entries that don't map to this node
               return new IteratorAsSpliterator.Builder<>(iterator())
                       .setEstimateRemaining(estimateSize < 0L ? Long.MAX_VALUE : estimateSize)
                       .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT | Spliterator.NONNULL)
                       .get();
            }

            @Override
            public int size() {
               long size = stream().count();
               if (size > Integer.MAX_VALUE) {
                  return Integer.MAX_VALUE;
               }
               return (int) size;
            }
         };
      }
      return set;
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      return enlistWriteAndInvokeNext(ctx, invalidateCommand);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }
   
   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return enlistReadAndInvokeNext(ctx, command);
   }

   private Object enlistReadAndInvokeNext(InvocationContext ctx, VisitableCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNextInterceptor(ctx, command);
   }

   private void enlistIfNeeded(InvocationContext ctx) throws SystemException {
      if (shouldEnlist(ctx)) {
         enlist((TxInvocationContext) ctx);
      }
   }

   private Object enlistWriteAndInvokeNext(InvocationContext ctx, WriteCommand command) throws Throwable {
      LocalTransaction localTransaction = null;
      if (shouldEnlist(ctx)) {
         localTransaction = enlist((TxInvocationContext) ctx);
         boolean implicitWith1Pc = useOnePhaseForAutoCommitTx && localTransaction.isImplicitTransaction();
         if (implicitWith1Pc) {
            //in this situation we don't support concurrent updates so skip locking entirely
            command.setFlags(Flag.SKIP_LOCKING);
         }
      }
      Object rv;
      try {
         rv = invokeNextInterceptor(ctx, command);
      } catch (OutdatedTopologyException e) {
         // The command will be retried, so we shouldn't mark the transaction for rollback
         throw e;
      } catch (Throwable throwable) {
         // Don't mark the transaction for rollback if it's fail silent (i.e. putForExternalRead)
         if (ctx.isOriginLocal() && ctx.isInTxScope() && !command.hasFlag(Flag.FAIL_SILENTLY)) {
            TxInvocationContext txCtx = (TxInvocationContext) ctx;
            txCtx.getTransaction().setRollbackOnly();
         }
         throw throwable;
      }
      if (localTransaction != null && command.isSuccessful()) {
         localTransaction.addModification(command);
      }
      return rv;
   }

   public LocalTransaction enlist(TxInvocationContext ctx) throws SystemException {
      Transaction transaction = ctx.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      if (isNotValid(status)) throw new IllegalStateException("Transaction " + transaction +
            " is not in a valid state to be invoking cache operations on.");
      LocalTransaction localTransaction = txTable.getLocalTransaction(transaction);
      txTable.enlist(transaction, localTransaction);
      return localTransaction;
   }

   private boolean isNotValid(int status) {
      return status != Status.STATUS_ACTIVE
            && status != Status.STATUS_PREPARING
            && status != Status.STATUS_COMMITTING;
   }

   private static boolean shouldEnlist(InvocationContext ctx) {
      return ctx.isInTxScope() && ctx.isOriginLocal();
   }

   @Override
   public boolean getStatisticsEnabled() {
      return isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset Statistics"
   )
   public void resetStatistics() {
      prepares.set(0);
      commits.set(0);
      rollbacks.set(0);
   }

   @ManagedAttribute(
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true
   )
   public boolean isStatisticsEnabled() {
      return this.statisticsEnabled;
   }

   @ManagedAttribute(
         description = "Number of transaction prepares performed since last reset",
         displayName = "Prepares",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getPrepares() {
      return prepares.get();
   }

   @ManagedAttribute(
         description = "Number of transaction commits performed since last reset",
         displayName = "Commits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getCommits() {
      return commits.get();
   }

   @ManagedAttribute(
         description = "Number of transaction rollbacks performed since last reset",
         displayName = "Rollbacks",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   public long getRollbacks() {
      return rollbacks.get();
   }

   private void verifyRemoteTransaction(RemoteTxInvocationContext ctx, AbstractTransactionBoundaryCommand command) throws Throwable {
      final GlobalTransaction globalTransaction = command.getGlobalTransaction();

      // command.getOrigin() and ctx.getOrigin() are not reliable for LockControlCommands started by
      // ClusteredGetCommands, or for PrepareCommands started by MultipleRpcCommands (when the replication queue
      // is enabled).
      final Address origin = globalTransaction.getAddress();

      //It is possible to receive a prepare or lock control command from a node that crashed. If that's the case rollback
      //the transaction forcefully in order to cleanup resources.
      boolean originatorMissing = !rpcManager.getTransport().getMembers().contains(origin);

      // It is also possible that the LCC timed out on the originator's end and this node has processed
      // a TxCompletionNotification.  So we need to check the presence of the remote transaction to
      // see if we need to clean up any acquired locks on our end.
      boolean alreadyCompleted = txTable.isTransactionCompleted(globalTransaction) || !txTable.containRemoteTx(globalTransaction);

      // We want to throw an exception if the originator left the cluster and the transaction is not finished
      // and/or it was rolled back by TransactionTable.cleanupLeaverTransactions().
      // We don't want to throw an exception if the originator left the cluster but the transaction already
      // completed successfully. So far, this only seems possible when forcing the commit of an orphaned
      // transaction (with recovery enabled).
      boolean completedSuccessfully = alreadyCompleted && !ctx.getCacheTransaction().isMarkedForRollback();

      boolean canRollback = command instanceof PrepareCommand && !((PrepareCommand) command).isOnePhaseCommit() ||
            command instanceof RollbackCommand || command instanceof LockControlCommand;

      if (trace) {
         log.tracef("invokeNextInterceptorAndVerifyTransaction :: originatorMissing=%s, alreadyCompleted=%s",
                    originatorMissing, alreadyCompleted);
      }

      if (alreadyCompleted || (originatorMissing && (canRollback || partitionHandlingManager.canRollbackTransactionAfterOriginatorLeave(globalTransaction)))) {
         if (trace) {
            log.tracef("Rolling back remote transaction %s because either already completed (%s) or originator no longer in the cluster (%s).",
                       globalTransaction, alreadyCompleted, originatorMissing);
         }
         RollbackCommand rollback = commandsFactory.buildRollbackCommand(command.getGlobalTransaction());
         try {
            invokeNextInterceptor(ctx, rollback);
         } finally {
            RemoteTransaction remoteTx = ctx.getCacheTransaction();
            remoteTx.markForRollback(true);
            txTable.removeRemoteTransaction(globalTransaction);
         }

         if (originatorMissing && !completedSuccessfully) {
            throw log.orphanTransactionRolledBack(globalTransaction);
         }
      }
   }

   private void replayRemoteTransactionIfNeeded(RemoteTxInvocationContext ctx, int topologyId) throws Throwable {
      // If a commit is received for a transaction that doesn't have its 'lookedUpEntries' populated
      // we know for sure this transaction is 2PC and was received via state transfer but the preceding PrepareCommand
      // was not received by local node because it was executed on the previous key owners. We need to re-prepare
      // the transaction on local node to ensure its locks are acquired and lookedUpEntries is properly populated.
      RemoteTransaction remoteTx = ctx.getCacheTransaction();
      if (trace) {
         log.tracef("Remote tx topology id %d and command topology is %d", remoteTx.lookedUpEntriesTopology(), topologyId);
      }
      if (remoteTx.lookedUpEntriesTopology() < topologyId) {
         PrepareCommand prepareCommand;
         if (useVersioning) {
            prepareCommand = commandsFactory.buildVersionedPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
         } else {
            prepareCommand = commandsFactory.buildPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
         }
         commandsFactory.initializeReplicableCommand(prepareCommand, true);
         prepareCommand.setOrigin(ctx.getOrigin());
         if (trace) {
            log.tracef("Replaying the transactions received as a result of state transfer %s", prepareCommand);
         }
         visitPrepareCommand(ctx, prepareCommand);
      }
   }

   static class TransactionAwareKeyCloseableIterator<K, V> extends TransactionAwareCloseableIterator<K, K, V> {
      private final Cache<K, V> cache;

      public TransactionAwareKeyCloseableIterator(CloseableIterator<K> realIterator,
                                                  TxInvocationContext<LocalTransaction> ctx, Cache<K, V> cache) {
         super(realIterator, ctx);
         this.cache = cache;
      }

      @Override
      protected K fromEntry(CacheEntry<K, V> entry) {
         return entry.getKey();
      }

      @Override
      protected Object getKey(K value) {
         return value;
      }

      @Override
      public void remove() {
         cache.remove(previousValue);
      }
   }

   static class TransactionAwareEntryCloseableIterator<K, V> extends TransactionAwareCloseableIterator<CacheEntry<K, V>, K, V> {
      private final Cache<K, V> cache;

      public TransactionAwareEntryCloseableIterator(CloseableIterator<CacheEntry<K, V>> realIterator,
                                                    TxInvocationContext<LocalTransaction> ctx, Cache<K, V> cache) {
         super(realIterator, ctx);
         this.cache = cache;
      }

      @Override
      public void remove() {
         cache.remove(previousValue.getKey(), previousValue.getValue());
      }

      @Override
      protected CacheEntry<K, V> fromEntry(CacheEntry<K, V> entry) {
         return entry;
      }

      @Override
      protected Object getKey(CacheEntry<K, V> value) {
         return value.getKey();
      }
   }

   /**
    * Class that provides transactional support so that the iterator will use the values in the context if they exist.
    * This will keep track of seen values from the transactional context and if the transactional context is updated while
    * iterating on this iterator it will see those updates unless the changed value was already seen by the iterator.
    *
    * @author wburns
    * @since 8.0
    */
   static abstract class TransactionAwareCloseableIterator<E, K, V> implements CloseableIterator<E> {
      private final TxInvocationContext<LocalTransaction> ctx;
      // We store all the not yet seen context entries here.  We rely on the fact that the cache entry reference is updated
      // if a change occurs in between iterations to see updates.
      private final List<CacheEntry> contextEntries;
      private final Set<Object> seenContextKeys = new HashSet<>();
      private final CloseableIterator<E> realIterator;

      protected E previousValue;
      protected E currentValue;

      public TransactionAwareCloseableIterator(CloseableIterator<E> realIterator,
                                               TxInvocationContext<LocalTransaction> ctx) {
         this.realIterator = realIterator;
         this.ctx = ctx;
         contextEntries = new ArrayList<>(ctx.getLookedUpEntries().values());
      }

      @Override
      public boolean hasNext() {
         if (currentValue == null) {
            currentValue = getNextFromIterator();
         }
         return currentValue != null;
      }

      @Override
      public E next() {
         E e = currentValue == null ? getNextFromIterator() : currentValue;
         if (e == null) {
            throw new NoSuchElementException();
         }
         currentValue = null;
         return e;
      }

      @Override
      public void close() {
         realIterator.close();
      }

      protected abstract E fromEntry(CacheEntry<K, V> entry);

      protected abstract Object getKey(E value);

      protected E getNextFromIterator() {
         E returnedValue = null;
         // We first have to exhaust all of our context entries
         CacheEntry<K, V> entry;
         while (returnedValue == null && !contextEntries.isEmpty() &&
                 (entry = contextEntries.remove(0)) != null) {
            seenContextKeys.add(entry.getKey());
            if (!ctx.isEntryRemovedInContext(entry.getKey()) && !entry.isNull()) {
               returnedValue = fromEntry(entry);
            }
         }
         if (returnedValue == null) {
            while (realIterator.hasNext()) {
               E iteratedEntry = realIterator.next();
               Object key = getKey(iteratedEntry);
               CacheEntry<K, V> contextEntry;
               // If the value was in the context then we ignore the stored value since we use the context value
               if ((contextEntry = ctx.lookupEntry(key)) != null) {
                  if (seenContextKeys.add(contextEntry.getKey()) && !contextEntry.isRemoved() && !contextEntry.isNull()) {
                     break;
                  }
               } else {
                  seenContextKeys.add(key);
                  // We have to add any entry we read from the iterator as if it was read from the context
                  // otherwise if the reader adds this entry to the context we will see it again
                  return iteratedEntry;
               }
            }
         }

         if (returnedValue == null) {
            // We do a last check to make sure no additional values were added to our context while iterating
            for (CacheEntry<K, V> lookedUpEntry : ctx.getLookedUpEntries().values()) {
               if (seenContextKeys.add(lookedUpEntry.getKey()) && !lookedUpEntry.isRemoved() && !lookedUpEntry.isNull()) {
                  if (returnedValue == null) {
                     returnedValue = fromEntry(lookedUpEntry);
                  } else {
                     contextEntries.add(lookedUpEntry);
                  }
               }
            }
         }
         return returnedValue;
      }
   }
}
