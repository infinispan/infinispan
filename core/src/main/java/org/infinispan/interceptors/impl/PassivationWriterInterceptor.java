package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.transaction.impl.AbstractCacheTransaction;

/**
 * Passivation writer ignores any create/modify operations and only does removals. The writes are done via eviction
 * or shutdown only.
 *
 * @author William Burns
 * @since 15.0
 */
public class PassivationWriterInterceptor extends CacheWriterInterceptor {

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
         isProperWriter(ctx, writeCommand, k) && v.isRemoved()));
      return asyncValue(batchStage);
   }

   @Override
   boolean shouldReplicateRemove(InvocationContext ctx, RemoveCommand removeCommand) {
      return removeCommand.isSuccessful();
   }
}
