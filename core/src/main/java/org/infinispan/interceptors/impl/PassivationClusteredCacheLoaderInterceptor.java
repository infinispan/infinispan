package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.DataOperationOrderer;

public class PassivationClusteredCacheLoaderInterceptor<K, V> extends ClusteredCacheLoaderInterceptor<K, V> {
   @Inject
   DataOperationOrderer orderer;
   @Inject ActivationManager activationManager;

   @Override
   public CompletionStage<InternalCacheEntry<K, V>> loadAndStoreInDataContainer(InvocationContext ctx, Object key,
                                                                                int segment, FlagAffectedCommand cmd) {
      Supplier<CompletionStage<InternalCacheEntry<K, V>>> supplier = () -> super.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      return PassivationCacheLoaderInterceptor.handlePassivationLoad(key, segment, orderer, activationManager, supplier);
   }
}
