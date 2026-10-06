package org.infinispan.interceptors.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
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

   private static final BiFunction<?, ?, ?> COMPLETE_NULL = (ignore, ignore2) -> null;
   private final InvocationSuccessFunction<WriteCommand> removeFromStoresAndContainer = this::removeFromStoresAndContainer;
   private final InvocationSuccessAction<WriteCommand> removeFromContainer = this::removeFromContainer;

   @Override
   Object removeEntriesAndReturn(InvocationContext ctx, WriteCommand command, Object rv) {
      List<?> keys = removedKeys(ctx, command);
      if (keys.isEmpty())
         return rv;

      // Hold a REMOVE slot for every removed key across the store deletes and the container removals.
      // A concurrent eviction of a key either completes first or observes the REMOVE and skips passivation, and once
      // the key leaves the container no eviction can select it again before the removal commits.
      // Waiting on every key while holding their slots cannot deadlock: evictions, activations and commits each order
      // on a single key, and two removals sharing keys are serialized by the key locks before reaching this point.
      List<CompletableFuture<DataOperationOrderer.Operation>> slots = new ArrayList<>(keys.size());
      AggregateCompletionStage<Void> previous = CompletionStages.aggregateCompletionStage();
      for (Object key : keys) {
         CompletableFuture<DataOperationOrderer.Operation> slot = new CompletableFuture<>();
         CompletionStage<DataOperationOrderer.Operation> ahead = orderer.orderOn(key, slot);
         // The orderer always completes a slot normally, we handle a predecessor that has completed exceptionally.
         if (ahead != null)
            previous.dependsOn(ahead.handle(completeNull()));
         slots.add(slot);
      }

      Object removeStage = asyncValue(previous.freeze())
            .thenApply(ctx, command, removeFromStoresAndContainer);

      // The slots open and close only within this method.
      // A removal that never commits, e.g., retried with a topology change, doesn't leave the slot occupied.
      // Also release on failures, so later operations can still proceed.
      return makeStage(removeStage).andHandle(ctx, command, (rCtx, rCommand, ignore, t) -> {
         // A failed removal leaves its keys in place, so it releases the slots as READ, a waiting eviction must still passivate the entry.
         // Completing with REMOVE with a failure would make the eviction drop an entry the removal didn't work.
         DataOperationOrderer.Operation outcome = t == null
               ? DataOperationOrderer.Operation.REMOVE
               : DataOperationOrderer.Operation.READ;
         for (int i = 0; i < keys.size(); i++) {
            orderer.completeOperation(keys.get(i), slots.get(i), outcome);
         }
         return valueOrException(rv, t);
      });
   }

   /**
    * Removes the keys {@code rCommand} removes from the cache stores and then from memory.
    * <p>
    * Runs once every removed key's predecessor in the orderer has completed, while this removal still holds the keys'
    * REMOVE slots. A key is taken out of memory only after its store delete succeeds.
    * </p>
    *
    * @param rCtx     the invocation context of {@code rCommand}.
    * @param rCommand the command removing the keys.
    * @param ignore   the result of waiting for the predecessors, unused.
    * @return an {@link InvocationStage} that completes once every removed key is out of the stores and out of memory, or
    *         with the first failure, in which case keys may remain in memory.
    */
   private Object removeFromStoresAndContainer(InvocationContext rCtx, WriteCommand rCommand, Object ignore) {
      return asyncValue(removeEntries(rCtx, rCommand)).thenAccept(rCtx, rCommand, removeFromContainer);
   }

   /**
    * Takes the keys {@code rCommand} removes out of the data container, ahead of the removal's commit.
    * <p>
    * Only keys whose entry is still removed in {@code rCtx} are taken out. Once they are out, no eviction can select
    * them, and the later commit of the removed entries finds nothing left to remove.
    * </p>
    *
    * @param rCtx     the invocation context of {@code rCommand}.
    * @param rCommand the command removing the keys.
    * @param ignore   the result of deleting the keys from the stores, unused.
    */
   private void removeFromContainer(InvocationContext rCtx, WriteCommand rCommand, Object ignore) {
      for (Object key : removedKeys(rCtx, rCommand)) {
         CacheEntry<?, ?> entry = rCtx.lookupEntry(key);
         if (entry != null && entry.isRemoved())
            dataContainer.remove(segmentOf(rCommand, key), key);
      }
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

   @SuppressWarnings("unchecked")
   private static <A, B, R> BiFunction<A, B, R> completeNull() {
      return (BiFunction<A, B, R>) COMPLETE_NULL;
   }
}
