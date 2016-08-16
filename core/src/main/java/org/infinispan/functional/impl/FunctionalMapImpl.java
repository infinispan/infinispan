package org.infinispan.functional.impl;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

import java.util.concurrent.ExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Status;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Functional map implementation.
 *
 * @since 8.0
 */
@Experimental
public final class FunctionalMapImpl<K, V> implements FunctionalMap<K, V> {

   final Params params;
   final AdvancedCache<K, V> cache;

   public static <K, V> FunctionalMapImpl<K, V> create(Params params, AdvancedCache<K, V> cache) {
      return new FunctionalMapImpl<>(params, cache);
   }

   public static <K, V> FunctionalMapImpl<K, V> create(AdvancedCache<K, V> cache) {
      return new FunctionalMapImpl<>(Params.create(), cache);
   }

   private FunctionalMapImpl(Params params, AdvancedCache<K, V> cache) {
      this.params = params;
      this.cache = cache;
   }

   InvocationContextFactory invCtxFactory() {
      return cache.getComponentRegistry().getComponent(InvocationContextFactory.class);
   }

   CommandsFactory cmdFactory() {
      return cache.getComponentRegistry().getComponent(CommandsFactory.class);
   }

   InterceptorChain chain() {
      return cache.getComponentRegistry().getComponent(InterceptorChain.class);
   }

   ExecutorService asyncExec() {
      return cache.getComponentRegistry().getComponent(ExecutorService.class, ASYNC_OPERATIONS_EXECUTOR);
   }

   FunctionalNotifier<K, V> notifier() {
      return cache.getComponentRegistry().getComponent(FunctionalNotifier.class);
   }

   @Override
   public FunctionalMapImpl<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), cache);
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public Status getStatus() {
      return toStatus(cache.getStatus());
   }

   @Override
   public void close() throws Exception {
      cache.stop();
   }

   private static Status toStatus(ComponentStatus cacheStatus) {
      return Status.valueOf(cacheStatus.name());
   }

}
