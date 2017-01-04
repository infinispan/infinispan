package org.infinispan.interceptors.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.function.TriFunction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * This interceptor acts as a replacement to the replication interceptor when the CacheImpl is configured with
 * ClusteredSyncMode as INVALIDATE.
 * <p/>
 * The idea is that rather than replicating changes to all caches in a cluster when write methods are called, simply
 * broadcast an {@link InvalidateCommand} on the remote caches containing all keys modified.  This allows the remote
 * cache to look up the value in a shared cache loader which would have been updated with the changes.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 9.0
 */
@MBean(objectName = "Invalidation", description = "Component responsible for invalidating entries on remote" +
      " caches when entries are written to locally.")
public class InvalidationInterceptor extends BaseRpcInterceptor implements JmxStatisticsExposer {
   private static final Log log = LogFactory.getLog(InvalidationInterceptor.class);

   private CommandsFactory commandsFactory;
   private boolean statisticsEnabled;

   private final AtomicLong invalidations = new AtomicLong(0);

   private final TriFunction<InvocationContext, ClearCommand, Object, InvocationStage>
         afterClearCommand = this::afterClearCommand;
   private final TriFunction<TxInvocationContext, PrepareCommand, Object, InvocationStage>
         afterPrepareCommand = this::afterPrepareCommand;
   private final TriFunction<LocalTxInvocationContext, LockControlCommand, Object, InvocationStage>
         afterLockControlCommand = this::afterLockControlCommand;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(CommandsFactory commandsFactory) {
      this.commandsFactory = commandsFactory;
   }

   @Start
   private void start() {
      this.setStatisticsEnabled(cacheConfiguration.jmxStatistics().enabled());
   }

   @Override
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!isPutForExternalRead(command)) {
         return handleInvalidate(ctx, command, command.getKey());
      }
      return invokeNext(ctx, command);
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleInvalidate(ctx, command, command.getKey());
   }

   @Override
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleInvalidate(ctx, command, command.getKey());
   }

   @Override
   public InvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenCompose(ctx, command, afterClearCommand);
   }

   private InvocationStage afterClearCommand(InvocationContext rCtx, ClearCommand rCommand, Object rv) {
      if (!isLocalModeForced(rCommand)) {
         // just broadcast the clear command - this is simplest!
         if (rCtx.isOriginLocal()) {
            CompletableFuture<Map<Address, Response>> remoteInvocation =
                  rpcManager.invokeRemotelyAsync(null, rCommand, getBroadcastRpcOptions(defaultSynchronous));
            return asyncStage(remoteInvocation.thenApply(responses -> null));
         }
      }
      return completedStage(rv);
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object[] keys = command.getMap() == null ? null : command.getMap().keySet().toArray();
      return handleInvalidate(ctx, command, keys);
   }

   @Override
   public InvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command)
            .thenCompose(ctx, command, afterPrepareCommand);
   }

   private InvocationStage afterPrepareCommand(TxInvocationContext rCtx, PrepareCommand rCommand, Object rv) {
      InvocationStage stage = completedStage(rv);
      log.tracef("Entering InvalidationInterceptor's prepare phase.  Ctx flags are empty");
      // fetch the modifications before the transaction is committed (and thus removed from the txTable)
      if (!shouldInvokeRemoteTxCommand(rCtx)) {
         log.tracef("Nothing to invalidate - no modifications in the transaction.");
         return stage;
      }

      if (rCtx.getTransaction() == null)
         throw new IllegalStateException("We must have an associated transaction");
      List<WriteCommand> mods = Arrays.asList(rCommand.getModifications());
      Collection<Object> remoteKeys = keysToInvalidateForPrepare(mods, rCtx);
      if (remoteKeys == null) {
         return stage;
      }
      CompletableFuture<Map<Address, Response>> remoteInvocation =
            invalidateAcrossCluster(defaultSynchronous, remoteKeys.toArray(), rCtx);
      return asyncStage(remoteInvocation.handle((responses, t) -> {
         if (t == null) {
            return null;
         }
         log.unableToRollbackEvictionsDuringPrepare(t);
         if (t instanceof RuntimeException)
            throw ((RuntimeException) t);
         else
            throw new RuntimeException("Unable to broadcast invalidation messages", t);
      }));
   }

   @Override
   public InvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNext(ctx, command)
            .thenCompose(ctx, command, (rCtx, rCommand, rv) -> {
         Set<Object> affectedKeys = rCtx.getAffectedKeys();
         log.tracef("On commit, send invalidate for keys: %s", affectedKeys);
               CompletableFuture<Map<Address, Response>> remoteInvocation;
         try {
            remoteInvocation = invalidateAcrossCluster(defaultSynchronous, affectedKeys.toArray(), rCtx);
            return asyncStage(remoteInvocation.handle((responses, t) -> {
               if (t != null) throw wrapException(t);

               return null;
            }));
         } catch (Throwable t) {
            throw wrapException(t);
         }
      });
   }

   private static RuntimeException wrapException(Throwable t) {
      if (t instanceof RuntimeException)
         return ((RuntimeException) t);
      else
         return log.unableToBroadcastInvalidation(t);
   }

   @Override
   public InvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command)
            .thenCompose((LocalTxInvocationContext) ctx, command, afterLockControlCommand);
   }

   private InvocationStage afterLockControlCommand(LocalTxInvocationContext rCtx, LockControlCommand rCommand,
                                                   Object ignored) {
      //unlock will happen async as it is a best effort
      boolean sync = !rCommand.isUnlock();
      rCtx.remoteLocksAcquired(rpcManager.getTransport()
                                         .getMembers());
      CompletableFuture<Map<Address, Response>> remoteInvocation =
            rpcManager.invokeRemotelyAsync(null, rCommand, getBroadcastRpcOptions(sync));
      return asyncStage(remoteInvocation.thenApply(responses -> null));
   }

   private InvocationStage handleInvalidate(InvocationContext ctx, WriteCommand command, Object... keys)
         throws Throwable {
      if (ctx.isInTxScope()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command)
            .thenCompose(ctx, command, (rCtx, rCommand, rv) -> invalidateAfterInvokeNext(rCtx, rCommand, rv, keys));
   }

   private InvocationStage invalidateAfterInvokeNext(InvocationContext ctx, WriteCommand command, Object rv,
                                                     Object[] keys) {
      if (command.isSuccessful()) {
         if (keys != null && keys.length != 0) {
            if (!isLocalModeForced(command)) {
               CompletableFuture<Map<Address, Response>> remoteInvocation =
                     invalidateAcrossCluster(isSynchronous(command), keys, ctx);
               return asyncStage(remoteInvocation).thenApply(rv, (rrv, responses) -> rrv);
            }
         }
      }
      return completedStage(rv);
   }

   private Collection<Object> keysToInvalidateForPrepare(List<WriteCommand> modifications,
                                                         InvocationContext ctx) {
      // A prepare does not carry flags, so skip checking whether is local or not
      if (!ctx.isInTxScope()) return null;
      if (modifications.isEmpty()) return null;

      InvalidationFilterVisitor filterVisitor = new InvalidationFilterVisitor(modifications.size());
      try {
         filterVisitor.visitCollection(ctx, modifications);
      } catch (Throwable throwable) {
         rethrowAsCompletedException(throwable);
      }

      if (filterVisitor.containsPutForExternalRead) {
         log.debug("Modification list contains a putForExternalRead operation.  Not invalidating.");
      } else if (filterVisitor.containsLocalModeFlag) {
         log.debug("Modification list contains a local mode flagged operation.  Not invalidating.");
      } else {
         return filterVisitor.result;
      }
      return null;
   }

   private static class InvalidationFilterVisitor extends AbstractVisitor {
      Set<Object> result;
      boolean containsPutForExternalRead = false;
      boolean containsLocalModeFlag = false;

      InvalidationFilterVisitor(int maxSetSize) {
         result = new HashSet<>(maxSetSize);
      }

      private void processCommand(FlagAffectedCommand command) {
         containsLocalModeFlag = containsLocalModeFlag || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         processCommand(command);
         containsPutForExternalRead = containsPutForExternalRead || command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ);
         result.add(command.getKey());
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         processCommand(command);
         result.add(command.getKey());
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         processCommand(command);
         result.addAll(command.getAffectedKeys());
         return null;
      }
   }

   private CompletableFuture<Map<Address, Response>> invalidateAcrossCluster(boolean synchronous, Object[] keys,
                                                                             InvocationContext ctx) {
      // increment invalidations counter if statistics maintained
      incrementInvalidations();
      final InvalidateCommand invalidateCommand = commandsFactory.buildInvalidateCommand(EnumUtil.EMPTY_BIT_SET, keys);
      if (log.isDebugEnabled())
         log.debug("Cache [" + rpcManager.getAddress() + "] replicating " + invalidateCommand);

      ReplicableCommand command = invalidateCommand;
      if (ctx.isInTxScope()) {
         TxInvocationContext txCtx = (TxInvocationContext) ctx;
         // A Prepare command containing the invalidation command in its 'modifications' list is sent to the remote nodes
         // so that the invalidation is executed in the same transaction and locks can be acquired and released properly.
         // This is 1PC on purpose, as an optimisation, even if the current TX is 2PC.
         // If the cache uses 2PC it's possible that the remotes will commit the invalidation and the originator rolls back,
         // but this does not impact consistency and the speed benefit is worth it.
         command = commandsFactory.buildPrepareCommand(txCtx.getGlobalTransaction(), Collections.singletonList(invalidateCommand), true);
      }
      return rpcManager.invokeRemotelyAsync(null, command, getBroadcastRpcOptions(synchronous));
   }

   private RpcOptions getBroadcastRpcOptions(boolean synchronous) {
      return rpcManager.getRpcOptionsBuilder(
            synchronous ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS).build();
   }

   private void incrementInvalidations() {
      if (statisticsEnabled) invalidations.incrementAndGet();
   }

   private boolean isPutForExternalRead(FlagAffectedCommand command) {
      if (command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         log.trace("Put for external read called.  Suppressing clustered invalidation.");
         return true;
      }
      return false;
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics"
   )
   public void resetStatistics() {
      invalidations.set(0);
   }

   @Override
   @ManagedAttribute(
         displayName = "Statistics enabled",
         description = "Enables or disables the gathering of statistics by this component",
         dataType = DataType.TRAIT,
         writable = true
   )
   public boolean getStatisticsEnabled() {
      return this.statisticsEnabled;
   }

   @Override
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean enabled) {
      this.statisticsEnabled = enabled;
   }

   @ManagedAttribute(
         description = "Number of invalidations",
         displayName = "Number of invalidations",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getInvalidations() {
      return invalidations.get();
   }
}
