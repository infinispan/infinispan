package org.infinispan.xsite;

import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.util.concurrent.CompletableFutures.asCompletionException;
import static org.infinispan.util.concurrent.CompletableFutures.completedExceptionFuture;
import static org.infinispan.util.logging.Log.XSITE;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * Common implementation logic for {@link org.infinispan.xsite.BackupReceiver}
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BaseBackupReceiver implements BackupReceiver {

   protected final AdvancedCache<Object, Object> cache;
   protected final ByteString cacheName;
   protected final TimeService timeService;
   private final DefaultHandler defaultHandler;
   private final AsyncBackupHandler asyncBackupHandler;

   BaseBackupReceiver(Cache<Object, Object> cache) {
      this.cache = cache.getAdvancedCache();
      this.cacheName = ByteString.fromString(cache.getName());
      ComponentRegistry registry = this.cache.getComponentRegistry();
      this.timeService = registry.getTimeService();
      ExecutorService executor = registry.getComponent(ExecutorService.class, BLOCKING_EXECUTOR);
      TransactionHandler txHandler = new TransactionHandler(cache);
      this.defaultHandler = new DefaultHandler(txHandler, executor);
      this.asyncBackupHandler = new AsyncBackupHandler(txHandler, executor, timeService);
   }

   static XSiteStatePushCommand newStatePushCommand(AdvancedCache<?, ?> cache, List<XSiteState> stateList) {
      CommandsFactory commandsFactory = cache.getComponentRegistry().getCommandsFactory();
      return commandsFactory.buildXSiteStatePushCommand(stateList.toArray(new XSiteState[0]), 0);
   }

   @Override
   public final Cache getCache() {
      return cache;
   }

   @Override
   public final CompletionStage<Void> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
      try {
         DefaultHandler visitor = preserveOrder ? asyncBackupHandler : defaultHandler;
         //noinspection unchecked
         return (CompletableFuture<Void>) command.acceptVisitor(null, visitor);
      } catch (Throwable throwable) {
         return completedExceptionFuture(throwable);
      }
   }


   final <T> CompletableFuture<T> checkInvocationAllowedFuture() {
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         return completedExceptionFuture(new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
      }
      return null;
   }

   final Void assertAllowInvocationFunction(Object ignoredRetVal) {
      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      ComponentStatus status = cache.getStatus();
      if (!status.allowInvocations()) {
         throw asCompletionException(new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
      }
      return null;
   }

   private static class DefaultHandler extends AbstractVisitor {

      final TransactionHandler txHandler;
      final ExecutorService executor;

      private DefaultHandler(TransactionHandler txHandler, ExecutorService executor) {
         this.txHandler = txHandler;
         this.executor = executor;
      }

      @Override
      public CompletionStage<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         return cache().putAsync(command.getKey(), command.getValue(), command.getMetadata());
      }

      @Override
      public CompletionStage<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         return cache().removeAsync(command.getKey());
      }

      @Override
      public CompletionStage<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
            WriteOnlyManyEntriesCommand command) {
         //noinspection unchecked
         return fMap().evalMany(command.getArguments(), MarshallableFunctions.setInternalCacheValueConsumer());
      }

      @Override
      public final CompletionStage<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) {
         return cache().clearAsync();
      }

      @Override
      public CompletionStage<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
         return CompletableFuture.runAsync(() -> txHandler.handlePrepareCommand(command), executor);
      }

      @Override
      public CompletionStage<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
         return CompletableFuture.runAsync(() -> txHandler.handleCommitCommand(command), executor);
      }

      @Override
      public CompletionStage<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         return CompletableFuture.runAsync(() -> txHandler.handleRollbackCommand(command), executor);
      }

      @Override
      protected final Object handleDefault(InvocationContext ctx, VisitableCommand command) {
         throw new UnsupportedOperationException();
      }

      private AdvancedCache<Object, Object> cache() {
         return txHandler.backupCache;
      }

      private FunctionalMap.WriteOnlyMap<Object, Object> fMap() {
         return txHandler.writeOnlyMap;
      }
   }

   private static final class AsyncBackupHandler extends DefaultHandler {

      private final ActionSequencer sequencer;

      private AsyncBackupHandler(TransactionHandler txHandler, ExecutorService executor, TimeService timeService) {
         super(txHandler, executor);
         sequencer = new ActionSequencer(executor, false, timeService);
      }

      @Override
      public CompletionStage<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         assert !command.isConditional();
         Callable<CompletionStage<Object>> action = () -> super.visitPutKeyValueCommand(null, command);
         return sequencer.orderOnKey(command.getKey(), action);
      }

      @Override
      public CompletionStage<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         assert !command.isConditional();
         Callable<CompletionStage<Object>> action = () -> super.visitRemoveCommand(null, command);
         return sequencer.orderOnKey(command.getKey(), action);
      }

      @Override
      public CompletionStage<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
            WriteOnlyManyEntriesCommand command) {
         Collection<?> keys = command.getAffectedKeys();
         Callable<CompletionStage<Void>> action = () -> super.visitWriteOnlyManyEntriesCommand(null, command);
         return sequencer.orderOnKeys(keys, action);
      }

      @Override
      public CompletionStage<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
         Collection<?> keys = command.getAffectedKeys();
         Callable<CompletionStage<Void>> action = () -> super.visitPrepareCommand(ctx, command);
         return sequencer.orderOnKeys(keys, action);
      }

      @Override
      public CompletionStage<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
         //we don't support async xsite with 2 phase commit
         throw new UnsupportedOperationException();
      }

      @Override
      public CompletionStage<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
         //we don't support async xsite with 2 phase commit
         throw new UnsupportedOperationException();
      }
   }

   // All conditional commands are unsupported
   private static final class TransactionHandler extends AbstractVisitor {

      private static final Log log = LogFactory.getLog(TransactionHandler.class);
      private static final boolean trace = log.isTraceEnabled();

      private final ConcurrentMap<GlobalTransaction, GlobalTransaction> remote2localTx;

      private final AdvancedCache<Object, Object> backupCache;
      private final FunctionalMap.WriteOnlyMap<Object, Object> writeOnlyMap;

      TransactionHandler(Cache<Object, Object> backup) {
         //ignore return values on the backup
         this.backupCache = backup.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
         this.writeOnlyMap = WriteOnlyMapImpl.create(FunctionalMapImpl.create(backupCache));
         this.remote2localTx = new ConcurrentHashMap<>();
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         if (command.isConditional()) {
            throw new UnsupportedOperationException();
         }
         backupCache.put(command.getKey(), command.getValue(), command.getMetadata());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         if (command.isConditional()) {
            throw new UnsupportedOperationException();
         }
         backupCache.remove(command.getKey());
         return null;
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
         CompletableFuture future = writeOnlyMap.evalMany(command.getArguments(), MarshallableFunctions.setInternalCacheValueConsumer());
         return future.join();
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) {
         throw new UnsupportedOperationException();
      }


      void handlePrepareCommand(PrepareCommand command) {
         if (isTransactional()) {
            // Sanity check -- if the remote tx doesn't have modifications, it never should have been propagated!
            if (!command.hasModifications()) {
               throw new IllegalStateException("TxInvocationContext has no modifications!");
            }

            try {
               replayModificationsInTransaction(command, command.isOnePhaseCommit());
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         } else {
            try {
               replayModifications(command);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      void handleCommitCommand(CommitCommand command) {
         if (!isTransactional()) {
            log.cannotRespondToCommit(command.getGlobalTransaction(), backupCache.getName());
         } else {
            if (trace) {
               log.tracef("Committing remote transaction %s", command.getGlobalTransaction());
            }
            try {
               completeTransaction(command.getGlobalTransaction(), true);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      void handleRollbackCommand(RollbackCommand command) {
         if (!isTransactional()) {
            log.cannotRespondToRollback(command.getGlobalTransaction(), backupCache.getName());
         } else {
            if (trace) {
               log.tracef("Rolling back remote transaction %s", command.getGlobalTransaction());
            }
            try {
               completeTransaction(command.getGlobalTransaction(), false);
            } catch (Throwable throwable) {
               throw CompletableFutures.asCompletionException(throwable);
            }
         }
      }

      private TransactionTable txTable() {
         return backupCache.getComponentRegistry().getComponent(TransactionTable.class);
      }

      private boolean isTransactional() {
         return backupCache.getCacheConfiguration().transaction().transactionMode() == TransactionMode.TRANSACTIONAL;
      }

      private void completeTransaction(GlobalTransaction globalTransaction, boolean commit) throws Throwable {
         TransactionTable txTable = txTable();
         GlobalTransaction localTxId = remote2localTx.remove(globalTransaction);
         if (localTxId == null) {
            throw XSITE.unableToFindRemoteSiteTransaction(globalTransaction);
         }
         LocalTransaction localTx = txTable.getLocalTransaction(localTxId);
         if (localTx == null) {
            throw XSITE.unableToFindLocalTransactionFromRemoteSiteTransaction(globalTransaction);
         }
         TransactionManager txManager = txManager();
         txManager.resume(localTx.getTransaction());
         if (!localTx.isEnlisted()) {
            if (trace) {
               log.tracef("%s isn't enlisted! Removing it manually.", localTx);
            }
            txTable().removeLocalTransaction(localTx);
         }
         if (commit) {
            txManager.commit();
         } else {
            txManager.rollback();
         }
      }

      private void replayModificationsInTransaction(PrepareCommand command, boolean onePhaseCommit) throws Throwable {
         TransactionManager tm = txManager();
         boolean replaySuccessful = false;
         try {

            tm.begin();
            replayModifications(command);
            replaySuccessful = true;
         } finally {
            LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
            if (localTx != null) { //possible for the tx to be null if we got an exception during applying modifications
               localTx.setFromRemoteSite(true);

               if (onePhaseCommit) {
                  if (replaySuccessful) {
                     if (trace) {
                        log.tracef("Committing remotely originated tx %s as it is 1PC", command.getGlobalTransaction());
                     }
                     tm.commit();
                  } else {
                     if (trace) {
                        log.tracef("Rolling back remotely originated tx %s", command.getGlobalTransaction());
                     }
                     tm.rollback();
                  }
               } else { // Wait for a remote commit/rollback.
                  remote2localTx.put(command.getGlobalTransaction(), localTx.getGlobalTransaction());
                  tm.suspend();
               }
            }
         }
      }

      private TransactionManager txManager() {
         return backupCache.getTransactionManager();
      }

      private void replayModifications(PrepareCommand command) throws Throwable {
         for (WriteCommand c : command.getModifications()) {
            c.acceptVisitor(null, this);
         }
      }
   }

}
