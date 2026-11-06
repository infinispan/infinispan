package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.concurrent.DataOperationOrderer;

/**
 * Passivation writer ignores any create/modify operations and only does removals. The writes are done via eviction
 * or shutdown only.
 *
 * @author William Burns
 * @since 15.0
 */
public class PassivationWriterInterceptor extends CacheWriterInterceptor {

   @Inject DataOperationOrderer orderer;
   @Inject InternalDataContainer<Object, Object> dataContainer;

   private final InvocationSuccessFunction<DataWriteCommand> removeFromStoreAndContainer = this::removeFromStoreAndContainer;
   private final InvocationSuccessAction<DataWriteCommand> removeFromContainer = this::removeFromContainer;

   /**
    * Removes the key targeted by {@code command} from the cache stores and from memory.
    * <p>
    * A concurrent eviction of the key does not write it back to the stores, so once the removal completes the removed
    * value cannot be read again.
    * </p>
    *
    * @param ctx     the invocation context of {@code command}.
    * @param command the command removing the key.
    * @param rv      the return value of {@code command}.
    * @return {@code rv}, or an {@link InvocationStage} that completes with {@code rv} once the key is removed from the
    *         stores and from memory, or with the failure if the removal fails.
    */
   @Override
   Object removeEntryAndReturn(InvocationContext ctx, DataWriteCommand command, Object rv) {
      // Hold a REMOVE slot for the key across the store delete and the container removal.
      // A concurrent eviction either completes first or observes the REMOVE and skips passivation, and once the key
      // leaves the container no eviction can select it again before the remove commits.
      CompletableFuture<DataOperationOrderer.Operation> future = new CompletableFuture<>();
      CompletionStage<DataOperationOrderer.Operation> previous = orderer.orderOn(command.getKey(), future);
      InvocationStage is = previous != null ? asyncValue(previous) : InvocationStage.completedNullStage();
      Object removeStage = is.thenApply(ctx, command, removeFromStoreAndContainer);

      // The slot opens and closes in this method, so a remove that never commits (e.g. retried after a topology change)
      // does not leave it held.
      // Released on failure as well, so later operations on the key are not blocked.
      return makeStage(removeStage).andHandle(ctx, command, (rCtx, rCommand, ignore, t) -> {
         orderer.completeOperation(rCommand.getKey(), future, DataOperationOrderer.Operation.REMOVE);
         return valueOrException(rv, t);
      });
   }

   private Object removeFromStoreAndContainer(InvocationContext rCtx, DataWriteCommand rCommand, Object ignore) {
      CompletionStage<?> cs = removeEntry(rCtx, rCommand.getKey(), rCommand.getSegment(), rCommand);
      return asyncValue(cs).thenAccept(rCtx, rCommand, removeFromContainer);
   }

   private void removeFromContainer(InvocationContext rCtx, DataWriteCommand rCommand, Object ignore) {
      // Removing the key here, ahead of the commit, closes the window where it is resident but no longer ordered.
      // Committing a removed entry only removes it from the container, so the later commit is a no-op.
      CacheEntry<?, ?> entry = rCtx.lookupEntry(rCommand.getKey());
      if (entry != null && entry.isRemoved())
         dataContainer.remove(rCommand.getSegment(), rCommand.getKey());
   }

   @Override
   CompletionStage<Void> storeEntry(InvocationContext ctx, Object key, FlagAffectedCommand command, boolean incrementStats) {
      return CompletableFutures.completedNull();
   }

   @Override
   protected Object handlePutMapCommandReturn(InvocationContext rCtx, PutMapCommand putMapCommand, Object rv) {
      return rv;
   }

   @Override
   protected InvocationStage store(TxInvocationContext<AbstractCacheTransaction> ctx) throws Throwable {
      CompletionStage<Long> batchStage = persistenceManager.performBatch(ctx, ((writeCommand, k, v) ->
            // Defensive null check for ISPN-16284
         isProperWriter(ctx, writeCommand, k) && v != null && v.isRemoved()));
      return asyncValue(batchStage);
   }

   @Override
   boolean shouldReplicateRemove(InvocationContext ctx, RemoveCommand removeCommand) {
      return removeCommand.isSuccessful();
   }
}
