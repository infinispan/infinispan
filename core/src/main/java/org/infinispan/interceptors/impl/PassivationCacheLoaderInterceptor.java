package org.infinispan.interceptors.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.DataOperationOrderer.Operation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject
   DataOperationOrderer orderer;
   @Inject ActivationManager activationManager;

   @Override
   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      Supplier<CompletionStage<InternalCacheEntry<K, V>>> supplier = () -> super.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      return handlePassivationLoad(key, segment, orderer, activationManager, supplier);
   }

   static <K, V> CompletionStage<InternalCacheEntry<K, V>> handlePassivationLoad(Object key, int segment,
                                                                                 DataOperationOrderer orderer,
                                                                                 ActivationManager activationManager,
                                                                                 Supplier<CompletionStage<InternalCacheEntry<K, V>>> supplier) {
      CompletableFuture<Operation> future = new CompletableFuture<>();
      CompletionStage<Operation> delayStage = orderer.orderOn(key, future);

      CompletionStage<InternalCacheEntry<K, V>> retrievalStage;
      if (delayStage != null) {
         retrievalStage = delayStage.thenCompose(ignore -> supplier.get());
      } else {
         retrievalStage = supplier.get();
      }
      return retrievalStage.whenComplete((value, t) -> {
         if (value != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Activating key: %s - not waiting for response", value.getKey());
            }
            // Note we don't wait on this to be removed, which allows the load to continue ahead. However
            // we can't release the orderer acquisition until the remove is complete
            activationManager.activateAsync(value.getKey(), segment)
                  .whenComplete((ignore, throwable) -> {
                     if (throwable != null) {
                        log.warnf("Activation of key %s failed for some reason", t);
                     }
                     orderer.completeOperation(key, future, Operation.READ);
                  });
         } else {
            orderer.completeOperation(key, future, Operation.READ);
         }
      });
   }
}
