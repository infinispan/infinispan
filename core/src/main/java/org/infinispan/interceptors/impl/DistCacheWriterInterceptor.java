package org.infinispan.interceptors.impl;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cache store interceptor specific for the distribution and replication cache modes.
 * <p>
 * <p>If the cache store is shared, only the primary owner of the key writes to the cache store.</p>
 * <p>If the cache store is not shared, every owner of a key writes to the cache store.</p>
 * <p>In non-tx caches, if the originator is an owner, the command is executed there twice. The first time,
 * ({@code isOriginLocal() == true}) we don't write anything to the cache store; the second time,
 * the normal rules apply.</p>
 * <p>For clear operations, either only the originator of the command clears the cache store (if it is
 * shared), or every node clears its cache store (if it is not shared). Note that in non-tx caches, this
 * happens without holding a lock on the primary owner of all the keys.</p>
 *
 * @author Galder Zamarreño
 * @author Dan Berindei
 * @since 9.0
 */
public class DistCacheWriterInterceptor extends CacheWriterInterceptor {
   private static final Log log = LogFactory.getLog(DistCacheWriterInterceptor.class);

   @Inject DistributionManager dm;

   private boolean isUsingLockDelegation;

   @Override
   protected Log getLog() {
      return log;
   }

   protected void start() {
      super.start();
      this.isUsingLockDelegation = !cacheConfiguration.transaction().transactionMode().isTransactional();
   }

   // ---- WRITE commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, putKeyValueCommand, rv) -> {
         Object key = putKeyValueCommand.getKey();
         if (!putKeyValueCommand.hasAnyFlag(FlagBitSets.ROLLING_UPGRADE) && (!isStoreEnabled(putKeyValueCommand) || rCtx.isInTxScope() || !putKeyValueCommand.isSuccessful()))
            return rv;
         if (!isProperWriter(rCtx, putKeyValueCommand, putKeyValueCommand.getKey()))
            return rv;

         return delayedValue(storeEntry(rCtx, key, putKeyValueCommand), rv);
      });
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return invokeNextThenApply(ctx, command, (rCtx, cmd, rv) -> {
         Object key = cmd.getKey();
         if (!isStoreEnabled(cmd) || !cmd.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, cmd, cmd.getKey()))
            return rv;

         return delayedValue(storeEntry(rCtx, key, cmd), rv);
      });
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (!isStoreEnabled(command) || ctx.isInTxScope())
         return invokeNext(ctx, command);

      return invokeNextThenApply(ctx, command, handlePutMapCommandReturn);
   }

   protected Object handlePutMapCommandReturn(InvocationContext rCtx, PutMapCommand putMapCommand, Object rv) {
      CompletionStage<Long> putMapStage = persistenceManager.writeMapCommand(putMapCommand, rCtx,
            ((writeCommand, key) -> !skipNonPrimary(rCtx, key, writeCommand) && isProperWriter(rCtx, writeCommand, key)));
      if (getStatisticsEnabled()) {
         putMapStage.thenAccept(cacheStores::getAndAdd);
      }
      return delayedValue(putMapStage, rv);
   }

   private boolean skipNonPrimary(InvocationContext rCtx, Object key, PutMapCommand command) {
      // In non-tx mode, a node may receive the same forwarded PutMapCommand many times - but each time
      // it must write only the keys locked on the primary owner that forwarded the rCommand
      return isUsingLockDelegation && command.isForwarded() && !dm.getCacheTopology().getDistribution(key).primary().equals(rCtx.getOrigin());
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, removeCommand, rv) -> {
         Object key = removeCommand.getKey();
         if (!isStoreEnabled(removeCommand) || rCtx.isInTxScope() || !removeCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, removeCommand, key))
            return rv;

         CompletionStage<?> stage = persistenceManager.deleteFromAllStores(key, removeCommand.getSegment(),
               skipSharedStores(rCtx, key, removeCommand) ? PRIVATE : BOTH);
         if (log.isTraceEnabled()) {
            stage = stage.thenAccept(removed ->
                  getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
         }
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, replaceCommand, rv) -> {
         Object key = replaceCommand.getKey();
         if (!isStoreEnabled(replaceCommand) || rCtx.isInTxScope() || !replaceCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, replaceCommand, replaceCommand.getKey()))
            return rv;

         return delayedValue(storeEntry(rCtx, key, replaceCommand), rv);
      });
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, computeCommand, rv) -> {
         Object key = computeCommand.getKey();
         if (!isStoreEnabled(computeCommand) || rCtx.isInTxScope() || !computeCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, computeCommand, computeCommand.getKey()))
            return rv;

         CompletionStage<?> stage;
         if (computeCommand.isSuccessful() && rv == null) {
             stage = persistenceManager.deleteFromAllStores(key, computeCommand.getSegment(),
                  skipSharedStores(rCtx, key, computeCommand) ? PRIVATE : BOTH);
            if (log.isTraceEnabled()) {
               stage = stage.thenAccept(removed ->
                     getLog().tracef("Removed entry under key %s and got response %s from CacheStore", key, removed));
            }
         } else if (computeCommand.isSuccessful()) {
            stage = storeEntry(rCtx, key, computeCommand);
         } else {
            stage = CompletableFutures.completedNull();
         }
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return invokeNextThenApply(ctx, command, (rCtx, computeIfAbsentCommand, rv) -> {
         Object key = computeIfAbsentCommand.getKey();
         if (!isStoreEnabled(computeIfAbsentCommand) || rCtx.isInTxScope() || !computeIfAbsentCommand.isSuccessful())
            return rv;
         if (!isProperWriter(rCtx, computeIfAbsentCommand, computeIfAbsentCommand.getKey()))
            return rv;

         return delayedValue(storeEntry(rCtx, key, computeIfAbsentCommand), rv);
      });
   }

   @Override
   protected boolean skipSharedStores(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return !dm.getCacheTopology().getDistribution(key).isPrimary() || command.hasAnyFlag(FlagBitSets.SKIP_SHARED_CACHE_STORE);
   }

   @Override
   protected boolean isProperWriter(InvocationContext ctx, FlagAffectedCommand command, Object key) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK))
         return true;

      int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);
      DistributionInfo distributionInfo = dm.getCacheTopology().getSegmentDistribution(segment);
      boolean nonTx = isUsingLockDelegation || command.hasAnyFlag(FlagBitSets.IRAC_UPDATE);
      if (nonTx && ctx.isOriginLocal() && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         // If the originator is a backup, the command will be forwarded back to it, and the value will be stored then
         // (while holding the lock on the primary owner).
         return distributionInfo.isPrimary();
      } else {
         return distributionInfo.isWriteOwner();
      }
   }
}
