package org.infinispan.interceptors.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.infinispan.util.concurrent.DataOperationOrderer.Operation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PassivationCacheLoaderInterceptor<K, V> extends CacheLoaderInterceptor<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject DataOperationOrderer orderer;

   // Normally the data container/store is updated in the order of updating the store and then the data container.
   // In doing so loading from the store and saving into the container is usually safe.
   // However, passivation with a concurrent remove can cause an issue with a read where the result is the
   // read value from the loader can resurrect the value into memory, thus we have to order the read to properly handle this case
   @Override
   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      CompletableFuture<Operation> future = new CompletableFuture<>();
      CompletionStage<Operation> delayStage = orderer.orderOn(key, future);

      CompletionStage<InternalCacheEntry<K, V>> retrievalStage;
      if (delayStage != null && !CompletionStages.isCompletedSuccessfully(delayStage)) {
         log.tracef("Found concurrent operation on key %s when attempting to load from store, waiting for its completion", key);
         retrievalStage = delayStage.thenCompose(ignore -> super.loadAndStoreInDataContainer(ctx, key, segment, cmd));
      } else {
         retrievalStage = super.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      }
      return retrievalStage.whenComplete((v, t) -> orderer.completeOperation(key, future, Operation.READ));
   }
}
