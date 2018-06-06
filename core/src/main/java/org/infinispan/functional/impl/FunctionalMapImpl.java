package org.infinispan.functional.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.Param;
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
   final Object lockOwner;
   final FunctionalNotifier notifier;
   final KeyPartitioner keyPartitioner;

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
      for (; ; ) {
         if (cache instanceof DecoratedCache) {
            flagsBitSet |= ((DecoratedCache) cache).getFlagsBitSet();
         }
         if (cache instanceof AbstractDelegatingCache) {
            cache = ((AbstractDelegatingCache) cache).getDelegate();
         } else {
            break;
         }
      }
      // By default the commands have Param.ReplicationMode.SYNC and this forces synchronous execution
      // We could either have third replication mode (USE_CACHE_MODE) or we enforce that here.
      if (!cache.getCacheConfiguration().clustering().cacheMode().isSynchronous()) {
         flagsBitSet |= FlagBitSets.FORCE_ASYNCHRONOUS;
      }
      return flagsBitSet;
   }

   // Finds the first decorated cache if there are delegates surrounding it otherwise null
   private DecoratedCache<K, V> findDecoratedCache(Cache<K, V> cache) {
      if (cache instanceof AbstractDelegatingCache) {
         if (cache instanceof DecoratedCache) {
            return ((DecoratedCache<K, V>) cache);
         }
         return findDecoratedCache(((AbstractDelegatingCache<K, V>) cache).getDelegate());
      }
      return null;
   }

   private FunctionalMapImpl(Params params, AdvancedCache<K, V> cache) {
      this.params = params;
      this.cache = cache;
      ComponentRegistry componentRegistry = cache.getComponentRegistry();
      chain = componentRegistry.getComponent(AsyncInterceptorChain.class);
      invCtxFactory = componentRegistry.getComponent(InvocationContextFactory.class);
      DecoratedCache<K, V> decoratedCache = findDecoratedCache(cache);
      lockOwner = decoratedCache == null ? null : decoratedCache.getLockOwner();
      commandsFactory = componentRegistry.getComponent(CommandsFactory.class);
      notifier = componentRegistry.getComponent(FunctionalNotifier.class);
      keyPartitioner = componentRegistry.getComponent(KeyPartitioner.class);
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
   public ComponentStatus getStatus() {
      return cache.getStatus();
   }

   @Override
   public void close() throws Exception {
      cache.stop();
   }
}
