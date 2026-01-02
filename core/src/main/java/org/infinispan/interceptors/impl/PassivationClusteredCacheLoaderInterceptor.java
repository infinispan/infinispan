package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.DataOperationOrderer;

public class PassivationClusteredCacheLoaderInterceptor<K, V> extends ClusteredCacheLoaderInterceptor<K, V> {
   @Inject DataOperationOrderer orderer;

   @Override
   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      CompletableFuture<DataOperationOrderer.Operation> future = new CompletableFuture<>();
      CompletionStage<DataOperationOrderer.Operation> delayStage = orderer.orderOn(key, future);

      CompletionStage<InternalCacheEntry<K, V>> retrievalStage;
      if (delayStage != null && !CompletionStages.isCompletedSuccessfully(delayStage)) {
         retrievalStage = delayStage.thenCompose(ignore -> super.loadAndStoreInDataContainer(ctx, key, segment, cmd));
      } else {
         retrievalStage = super.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      }

      return retrievalStage.whenComplete((v, t) -> orderer.completeOperation(key, future, DataOperationOrderer.Operation.READ));
   }
}
