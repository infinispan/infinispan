package org.infinispan.interceptors.impl;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.Caches;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
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
 * @since 9.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor<K, V> extends DDAsyncInterceptor implements JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(TxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final AtomicLong prepares = new AtomicLong(0);
   private final AtomicLong commits = new AtomicLong(0);
   private final AtomicLong rollbacks = new AtomicLong(0);

   private CommandsFactory commandsFactory;
   private Cache<K, V> cache;
   private RecoveryManager recoveryManager;
   private TransactionTable txTable;

   private boolean isTotalOrder;
   private boolean useOnePhaseForAutoCommitTx;
   private boolean useVersioning;
   private boolean statisticsEnabled;

   @Inject
   public void init(TransactionTable txTable, Configuration configuration, RecoveryManager recoveryManager,
         CommandsFactory commandsFactory, Cache<K, V> cache) {
      this.cacheConfiguration = configuration;
      this.txTable = txTable;
      this.recoveryManager = recoveryManager;
      this.commandsFactory = commandsFactory;
      this.cache = cache;

      statisticsEnabled = cacheConfiguration.jmxStatistics().enabled();
      isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      useOnePhaseForAutoCommitTx = cacheConfiguration.transaction().use1PcForAutoCommitTransactions();
      useVersioning = Configurations.isTxVersioned(configuration);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handlePrepareCommand(ctx, command);
   }

   private Object handlePrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      // Debugging for ISPN-5379
      ctx.getCacheTransaction().freezeModifications();

      //if it is remote and 2PC then first log the tx only after replying mods
      if (this.statisticsEnabled) prepares.incrementAndGet();
      if (!ctx.isOriginLocal()) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());
         Object verifyResult = invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            if (!rCtx.isOriginLocal()) {
               return verifyRemoteTransaction((RemoteTxInvocationContext) rCtx,
                                              (AbstractTransactionBoundaryCommand) rCommand, rv, throwable);
            }

            return valueOrException(rv, throwable);
         });
         return makeStage(verifyResult).thenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            PrepareCommand prepareCommand = (PrepareCommand) rCommand;
            if (prepareCommand.isOnePhaseCommit()) {
               txTable.remoteTransactionCommitted(prepareCommand.getGlobalTransaction(), true);
            } else {
               txTable.remoteTransactionPrepared(prepareCommand.getGlobalTransaction());
            }
         });
      } else {
         if (ctx.getCacheTransaction().hasModification(ClearCommand.class)) {
            throw new IllegalStateException("No ClearCommand is allowed in Transaction.");
         }
         return invokeNext(ctx, command);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      // TODO The local origin check is needed for CommitFailsTest, but it doesn't appear correct to roll back an in-doubt tx
      if (!ctx.isOriginLocal()) {
         GlobalTransaction gtx = ctx.getGlobalTransaction();
         if (txTable.isTransactionCompleted(gtx)) {
            if (trace) log.tracef("Transaction %s already completed, skipping commit", gtx);
            return null;
         }

         if (!isTotalOrder) {
            InvocationStage replayStage = replayRemoteTransactionIfNeeded((RemoteTxInvocationContext) ctx,
                  command.getTopologyId());
            if (replayStage != null) {
               return replayStage.andHandle(ctx, command, (rCtx, rCommand, rv, t) ->
                     finishCommit((TxInvocationContext<?>) rCtx, rCommand));
            } else {
               return finishCommit(ctx, command);
            }
         }
      }

      return finishCommit(ctx, command);
   }

   private Object finishCommit(TxInvocationContext<?> ctx, VisitableCommand command) {
      GlobalTransaction gtx = ctx.getGlobalTransaction();
      if (this.statisticsEnabled) commits.incrementAndGet();
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         if (!rCtx.isOriginLocal() || isTotalOrder) {
            txTable.remoteTransactionCommitted(gtx, false);
         }
      });
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (this.statisticsEnabled) rollbacks.incrementAndGet();
      // The transaction was marked as completed in RollbackCommand.prepare()
      if (!ctx.isOriginLocal() || isTotalOrder) {
         txTable.remoteTransactionRollback(command.getGlobalTransaction());
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         //for tx that rollback we do not send a TxCompletionNotification, so we should cleanup
         // the recovery info here
         if (recoveryManager != null) {
            GlobalTransaction gtx = ((RollbackCommand) rCommand).getGlobalTransaction();
            recoveryManager.removeRecoveryInformation(((RecoverableTransactionIdentifier) gtx).getXid());
         }
      });
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);

      if (ctx.isOriginLocal()) {
         command.setGlobalTransaction(ctx.getGlobalTransaction());
      }

      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         if (!rCtx.isOriginLocal()) {
            return verifyRemoteTransaction((RemoteTxInvocationContext) rCtx,
                                           (AbstractTransactionBoundaryCommand) rCommand, rv, throwable);
         }
         return valueOrException(rv, throwable);
      });
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (rCtx.isInTxScope()) {
            CacheSet<K> set = (CacheSet<K>) rv;
            return new AbstractDelegatingKeyCacheSet(Caches.getCacheWithFlags(cache, (FlagAffectedCommand) rCommand), set) {
               @Override
               public CloseableIterator<K> iterator() {
                  return new TransactionAwareKeyCloseableIterator<>(super.iterator(),
                        (TxInvocationContext<LocalTransaction>) rCtx, cache);
               }

               @Override
               public CloseableSpliterator<K> spliterator() {
                  Spliterator<K> parentSpliterator = super.spliterator();
                  long estimateSize =
                        parentSpliterator.estimateSize() + rCtx.getLookedUpEntries().size();
                  // This is an overestimate for size if we have looked up entries that don't map to
                  // this node
                  return new IteratorAsSpliterator.Builder<>(iterator())
                        .setEstimateRemaining(estimateSize < 0L ? Long.MAX_VALUE : estimateSize)
                        .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT |
                              Spliterator.NONNULL).get();
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
         return rv;
      });
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (rCtx.isInTxScope()) {
            CacheSet<CacheEntry<K, V>> set = (CacheSet<CacheEntry<K, V>>) rv;
            return new AbstractDelegatingEntryCacheSet<K, V>(
                  Caches.getCacheWithFlags(cache, (FlagAffectedCommand) rCommand), set) {
               @Override
               public CloseableIterator<CacheEntry<K, V>> iterator() {
                  return new TransactionAwareEntryCloseableIterator<>(super.iterator(),
                        (TxInvocationContext<LocalTransaction>) rCtx, cache);
               }

               @Override
               public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
                  Spliterator<CacheEntry<K, V>> parentSpliterator = super.spliterator();
                  long estimateSize =
                        parentSpliterator.estimateSize() + rCtx.getLookedUpEntries().size();
                  // This is an overestimate for size if we have looked up entries that don't map to
                  // this node
                  return new IteratorAsSpliterator.Builder<>(iterator())
                        .setEstimateRemaining(estimateSize < 0L ? Long.MAX_VALUE : estimateSize)
                        .setCharacteristics(Spliterator.CONCURRENT | Spliterator.DISTINCT |
                              Spliterator.NONNULL).get();
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
         return rv;
      });
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand)
         throws Throwable {
      return handleWriteCommand(ctx, invalidateCommand);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   private void enlistIfNeeded(InvocationContext ctx) throws SystemException {
      if (shouldEnlist(ctx)) {
         enlist((TxInvocationContext) ctx);
      }
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      enlistIfNeeded(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command)
         throws Throwable {
      if (shouldEnlist(ctx)) {
         LocalTransaction localTransaction = enlist((TxInvocationContext) ctx);
         boolean implicitWith1Pc = useOnePhaseForAutoCommitTx && localTransaction.isImplicitTransaction();
         if (implicitWith1Pc) {
            //in this situation we don't support concurrent updates so skip locking entirely
            command.addFlags(FlagBitSets.SKIP_LOCKING);
         }
      }
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         // We shouldn't mark the transaction for rollback if it's going to be retried
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (t != null && !(t instanceof OutdatedTopologyException)) {
            // Don't mark the transaction for rollback if it's fail silent (i.e. putForExternalRead)
            if (rCtx.isOriginLocal() && rCtx.isInTxScope() && !writeCommand.hasAnyFlag(FlagBitSets.FAIL_SILENTLY)) {
               TxInvocationContext txCtx = (TxInvocationContext) rCtx;
               txCtx.getTransaction().setRollbackOnly();
            }
         }
         if (t == null && shouldEnlist(rCtx) && writeCommand.isSuccessful()) {
            TxInvocationContext<LocalTransaction> txContext = (TxInvocationContext<LocalTransaction>) rCtx;
            txContext.getCacheTransaction().addModification(writeCommand);
         }
      });
   }

   public LocalTransaction enlist(TxInvocationContext ctx) throws SystemException {
      Transaction transaction = ctx.getTransaction();
      if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
      int status = transaction.getStatus();
      LocalTransaction localTransaction = (LocalTransaction) ctx.getCacheTransaction();
      if (isNotValid(status)) {
         if (!localTransaction.isEnlisted()) {
            // This transaction wouldn't be removed by TM.commit() or TM.rollback()
            txTable.removeLocalTransaction(localTransaction);
         }
         throw new IllegalStateException("Transaction " + transaction +
                                               " is not in a valid state to be invoking cache operations on.");
      }
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

   @Override
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

   private Object verifyRemoteTransaction(RemoteTxInvocationContext ctx, AbstractTransactionBoundaryCommand command,
                                          Object rv, Throwable throwable) throws Throwable {
      final GlobalTransaction globalTransaction = command.getGlobalTransaction();

      // It is also possible that the LCC timed out on the originator's end and this node has processed
      // a TxCompletionNotification.  So we need to check the presence of the remote transaction to
      // see if we need to clean up any acquired locks on our end.
      boolean alreadyCompleted = txTable.isTransactionCompleted(globalTransaction) || !txTable.containRemoteTx(globalTransaction);

      boolean canRollback = command instanceof PrepareCommand && !((PrepareCommand) command).isOnePhaseCommit() ||
            command instanceof LockControlCommand;

      if (trace) {
         log.tracef("Verifying transaction: alreadyCompleted=%s", alreadyCompleted);
      }

      if (alreadyCompleted) {
         if (trace) {
            log.tracef("Rolling back remote transaction %s because it was already completed",
                       globalTransaction);
         }
         // The rollback command only marks the transaction as completed in invokeAsync()
         txTable.markTransactionCompleted(globalTransaction, false);
         RollbackCommand rollback = commandsFactory.buildRollbackCommand(command.getGlobalTransaction());
         return invokeNextAndFinally(ctx, rollback, (rCtx, rCommand, rv1, throwable1) -> {
            RemoteTransaction remoteTx = ((TxInvocationContext<RemoteTransaction>) rCtx).getCacheTransaction();
            remoteTx.markForRollback(true);
            txTable.removeRemoteTransaction(globalTransaction);
         });
      }

      return valueOrException(rv, throwable);
   }

   private InvocationStage replayRemoteTransactionIfNeeded(RemoteTxInvocationContext ctx, int topologyId)
         throws Throwable {
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
            log.tracef("Replaying the transactions received as a result of state transfer %s",
                  prepareCommand);
         }
         return makeStage(handlePrepareCommand(ctx, prepareCommand));
      }
      return null;
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
         if (previousValue == null) {
            throw new IllegalStateException();
         }
         cache.remove(previousValue);
         previousValue = null;
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
         if (previousValue == null) {
            throw new IllegalStateException();
         }
         cache.remove(previousValue.getKey(), previousValue.getValue());
         previousValue = null;
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
      private final Deque<CacheEntry> contextEntries;
      private final Set<Object> seenContextKeys = new HashSet<>();
      private final CloseableIterator<E> realIterator;

      protected E previousValue;
      protected E currentValue;

      public TransactionAwareCloseableIterator(CloseableIterator<E> realIterator,
                                               TxInvocationContext<LocalTransaction> ctx) {
         this.realIterator = realIterator;
         this.ctx = ctx;
         contextEntries = new ArrayDeque<>(ctx.getLookedUpEntries().values());
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
         previousValue = e;
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
                 (entry = contextEntries.poll()) != null) {
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
