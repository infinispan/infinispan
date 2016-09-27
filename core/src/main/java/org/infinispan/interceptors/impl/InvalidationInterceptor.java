package org.infinispan.interceptors.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
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
   private final AtomicLong invalidations = new AtomicLong(0);
   private CommandsFactory commandsFactory;
   private boolean statisticsEnabled;

   private static final Log log = LogFactory.getLog(InvalidationInterceptor.class);

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
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!isPutForExternalRead(command)) {
         return handleInvalidate(ctx, command, command.getKey());
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleInvalidate(ctx, command, command.getKey());
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleInvalidate(ctx, command, command.getKey());
   }

   @Override
   public BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         ClearCommand clearCommand = (ClearCommand) rCommand;
         if (!isLocalModeForced(clearCommand)) {
            // just broadcast the clear command - this is simplest!
            if (rCtx.isOriginLocal())
               rpcManager.invokeRemotely(null, clearCommand, getBroadcastRpcOptions(defaultSynchronous));
         }
      });
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object[] keys = command.getMap() == null ? null : command.getMap().keySet().toArray();
      return handleInvalidate(ctx, command, keys);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         log.tracef("Entering InvalidationInterceptor's prepare phase.  Ctx flags are empty");
         // fetch the modifications before the transaction is committed (and thus removed from the txTable)
         TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
         if (shouldInvokeRemoteTxCommand(txInvocationContext)) {
            if (txInvocationContext.getTransaction() == null)
               throw new IllegalStateException("We must have an associated transaction");
            PrepareCommand prepareCommand = (PrepareCommand) rCommand;
            List<WriteCommand> mods = Arrays.asList(prepareCommand.getModifications());
            broadcastInvalidateForPrepare(mods, txInvocationContext);
         } else {
            log.tracef("Nothing to invalidate - no modifications in the transaction.");
         }
      });
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         Set<Object> affectedKeys = ctx.getAffectedKeys();
         try {
            log.tracef("On commit, send invalidate for keys: %s", affectedKeys);
            invalidateAcrossCluster(defaultSynchronous, affectedKeys.toArray(), rCtx);
         } catch (Throwable t) {
            if (t instanceof RuntimeException)
               throw t;
            else
               throw log.unableToBroadcastInvalidation(t);
         }
      });
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         //unlock will happen async as it is a best effort
         LockControlCommand lockControlCommand = (LockControlCommand) rCommand;
         boolean sync = !lockControlCommand.isUnlock();
         ((LocalTxInvocationContext) rCtx).remoteLocksAcquired(rpcManager.getTransport().getMembers());
         rpcManager.invokeRemotely(null, lockControlCommand, getBroadcastRpcOptions(sync));
      });
   }

   private InvocationStage handleInvalidate(InvocationContext ctx, WriteCommand command, Object... keys)
         throws Throwable {
      if (ctx.isInTxScope()) {
         return invokeNext(ctx, command);
      }
      return invokeNext(ctx, command).thenAccept((rCtx, rCommand, rv) -> {
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (writeCommand.isSuccessful()) {
            if (keys != null && keys.length != 0) {
               if (!isLocalModeForced(writeCommand))
                  invalidateAcrossCluster(isSynchronous(writeCommand), keys, rCtx);
            }
         }
      });
   }

   private void broadcastInvalidateForPrepare(List<WriteCommand> modifications, InvocationContext ctx)
         throws Throwable {
      // A prepare does not carry flags, so skip checking whether is local or not
      if (ctx.isInTxScope()) {
         if (modifications.isEmpty()) return;
         InvalidationFilterVisitor filterVisitor = new InvalidationFilterVisitor(modifications.size());
         filterVisitor.visitCollection(ctx, modifications);

         if (filterVisitor.containsPutForExternalRead) {
            log.debug("Modification list contains a putForExternalRead operation.  Not invalidating.");
         } else if (filterVisitor.containsLocalModeFlag) {
            log.debug("Modification list contains a local mode flagged operation.  Not invalidating.");
         } else {
            try {
               invalidateAcrossCluster(defaultSynchronous, filterVisitor.result.toArray(), ctx);
            } catch (Throwable t) {
               log.unableToRollbackEvictionsDuringPrepare(t);
               if (t instanceof RuntimeException)
                  throw t;
               else
                  throw new RuntimeException("Unable to broadcast invalidation messages", t);
            }
         }
      }
   }

   public static class InvalidationFilterVisitor extends AbstractVisitor {
      Set<Object> result;
      public boolean containsPutForExternalRead = false;
      public boolean containsLocalModeFlag = false;

      public InvalidationFilterVisitor(int maxSetSize) {
         result = new HashSet<Object>(maxSetSize);
      }

      private void processCommand(FlagAffectedCommand command) {
         containsLocalModeFlag = containsLocalModeFlag || (command.getFlags() != null && command.getFlags().contains(Flag.CACHE_MODE_LOCAL));
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         processCommand(command);
         containsPutForExternalRead = containsPutForExternalRead || (command.getFlags() != null && command.getFlags().contains(Flag.PUT_FOR_EXTERNAL_READ));
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

   private void invalidateAcrossCluster(boolean synchronous, Object[] keys, InvocationContext ctx) throws Throwable {
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
      rpcManager.invokeRemotely(null, command, getBroadcastRpcOptions(synchronous));
   }

   private RpcOptions getBroadcastRpcOptions(boolean synchronous) {
      return rpcManager.getRpcOptionsBuilder(
            synchronous ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS).build();
   }

   private void incrementInvalidations() {
      if (statisticsEnabled) invalidations.incrementAndGet();
   }

   private boolean isPutForExternalRead(FlagAffectedCommand command) {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
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
