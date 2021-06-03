package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.DataOperationOrderer;

public class PassivationClusteredCacheLoaderInterceptor<K, V> extends ClusteredCacheLoaderInterceptor<K, V> {
   @Inject DataOperationOrderer orderer;
   @Inject ActivationManager activationManager;

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
       if (CompletionStages.isCompletedSuccessfully(retrievalStage)) {
           InternalCacheEntry<K, V> ice = CompletionStages.join(retrievalStage);
           PassivationCacheLoaderInterceptor.activateAfterLoad(key, segment, orderer, activationManager, future, ice, null);
           return retrievalStage;
       } else {
           return retrievalStage.whenComplete((value, t) -> {
               PassivationCacheLoaderInterceptor.activateAfterLoad(key, segment, orderer, activationManager, future, value, t);
           });
       }
   }
}
