package org.infinispan.functional.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.Traversable;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * A {@link ReadOnlyMapImpl} that works with a simple cache.
 *
 * @since 15.0
 * @author José Bolina
 * @see ReadOnlyMapImpl
 */
@Experimental
public class SimpleReadOnlyMapImpl<K, V> extends ReadOnlyMapImpl<K, V> {
   private static final String KEY_CANNOT_BE_NULL = "Key cannot be null";
   private static final String FUNCTION_CANNOT_BE_NULL = "Function cannot be null";

   private final CacheNotifier<K, V> notifier;


   SimpleReadOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
      this.notifier = functionalMap.cache.getComponentRegistry().getComponent(CacheNotifier.class);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<EntryView.ReadEntryView<K, V>, R> f) {
      try {
         return internalEval(key, f);
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<EntryView.ReadEntryView<K, V>, R> f) {
      List<R> results = new ArrayList<>(keys.size());
      for (K key : keys) {
         // Everything is local, CF is always complete.
         CompletableFuture<R> cf = eval(key, f);
         assert cf.isDone() : "eval() returned an incomplete CF";

         R r = CompletionStages.join(cf);
         results.add(r);
      }
      return Traversables.of(results.stream());
   }

   private <R> CompletableFuture<R> internalEval(K key, Function<EntryView.ReadEntryView<K, V>, R> f) {
      Objects.requireNonNull(key, KEY_CANNOT_BE_NULL);
      Objects.requireNonNull(f, FUNCTION_CANNOT_BE_NULL);

      InternalCacheEntry<K, V> ice = fmap.cache.getDataContainer().peek(key);
      boolean notify = false;
      EntryView.ReadEntryView<K, V> view;
      if (ice == null || ice.isNull()) {
         view = EntryViews.noValue(key);
      } else {
         view = EntryViews.readOnly(ice);
         notify = true;
      }

      if (notify && hasListeners())
         CompletionStages.join(notifier.notifyCacheEntryVisited(ice.getKey(), ice.getValue(), true, ImmutableContext.INSTANCE, null));

      R r = EntryViews.snapshot(f.apply(view));

      if (notify && hasListeners())
         CompletionStages.join(notifier.notifyCacheEntryVisited(ice.getKey(), ice.getValue(), false, ImmutableContext.INSTANCE, null));

      if (r == null) return CompletableFutures.completedNull();
      return CompletableFuture.completedFuture(r);
   }

   private boolean hasListeners() {
      return !notifier.getListeners().isEmpty();
   }
}
