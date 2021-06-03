package org.infinispan.interceptors.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.DataOperationOrderer.Operation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   @Inject
   DataOperationOrderer orderer;
   @Inject
   ActivationManager activationManager;

   @Override
   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      CompletableFuture<Operation> future = new CompletableFuture<>();
      CompletionStage<Operation> delayStage = orderer.orderOn(key, future);

      CompletionStage<InternalCacheEntry<K, V>> retrievalStage;
      if (delayStage != null && !CompletionStages.isCompletedSuccessfully(delayStage)) {
         retrievalStage = delayStage.thenCompose(ignore -> super.loadAndStoreInDataContainer(ctx, key, segment, cmd));
      } else {
         retrievalStage = super.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      }
      if (CompletionStages.isCompletedSuccessfully(retrievalStage)) {
         InternalCacheEntry<K, V> ice = CompletionStages.join(retrievalStage);
         activateAfterLoad(key, segment, orderer, activationManager, future, ice, null);
         return retrievalStage;
      } else {
         return retrievalStage.whenComplete((value, t) -> {
            activateAfterLoad(key, segment, orderer, activationManager, future, value, t);
         });
      }
   }

   static <K, V> void activateAfterLoad(Object key, int segment, DataOperationOrderer orderer, ActivationManager activationManager, CompletableFuture<Operation> future, InternalCacheEntry<K, V> value, Throwable t) {
      if (value != null) {
         if (trace) {
            log.tracef("Activating key: %s - not waiting for response", value.getKey());
         }
         // Note we don't wait on this to be removed, which allows the load to continue ahead.
         // However, we can't release the orderer acquisition until the remove is complete
         CompletionStage<Void> activationStage = activationManager.activateAsync(value.getKey(), segment);
         if (!CompletionStages.isCompletedSuccessfully(activationStage)) {
            activationStage.whenComplete((ignore, throwable) -> {
               if (throwable != null) {
                  log.warnf("Activation of key %s failed for some reason", t);
               }
               orderer.completeOperation(key, future, Operation.READ);
            });
         } else {
            orderer.completeOperation(key, future, Operation.READ);
         }
      } else {
         orderer.completeOperation(key, future, Operation.READ);
      }
   }
}
