package org.infinispan.functional.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Status;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
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
   final AsyncInterceptorChain chain;
   final CommandsFactory commandsFactory;
   final InvocationContextFactory invCtxFactory;
   final FunctionalNotifier notifier;

   public static <K, V> FunctionalMapImpl<K, V> create(Params params, AdvancedCache<K, V> cache) {
      params = params.addAll(Params.fromFlagsBitSet(getFlagsBitSet(cache)));
      return new FunctionalMapImpl<>(params, cache);
   }

   public static <K, V> FunctionalMapImpl<K, V> create(AdvancedCache<K, V> cache) {
      Params params = Params.fromFlagsBitSet(getFlagsBitSet(cache));
      return new FunctionalMapImpl<>(params, cache);
   }

   private static <K, V> long getFlagsBitSet(Cache<K, V> cache) {
      long flagsBitSet = 0;
      for (;;) {
         if (cache instanceof DecoratedCache) {
            flagsBitSet |= ((DecoratedCache) cache).getFlagsBitSet();
         }
         if (cache instanceof AbstractDelegatingCache) {
            cache = ((AbstractDelegatingCache) cache).getDelegate();
         } else {
            break;
         }
      }
      return flagsBitSet;
   }

   private FunctionalMapImpl(Params params, AdvancedCache<K, V> cache) {
      this.params = params;
      this.cache = cache;
      ComponentRegistry componentRegistry = cache.getComponentRegistry();
      chain = componentRegistry.getComponent(AsyncInterceptorChain.class);
      invCtxFactory = componentRegistry.getComponent(InvocationContextFactory.class);
      commandsFactory = componentRegistry.getComponent(CommandsFactory.class);
      notifier = componentRegistry.getComponent(FunctionalNotifier.class);
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
