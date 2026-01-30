package org.infinispan.functional.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.Traversable;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;

/**
 * A {@link ReadWriteMapImpl} that works with a simple cache.
 *
 * @author José Bolina
 * @see ReadWriteMapImpl
 * @since 15.0
 */
@Experimental
public class SimpleReadWriteMapImpl<K, V> extends ReadWriteMapImpl<K, V> implements SimpleFunctionalMap<K, V> {
   private static final String KEY_CANNOT_BE_NULL = "Key cannot be null";
   private static final String FUNCTION_CANNOT_BE_NULL = "Function cannot be null";

   private final InternalExpirationManager<K, V> expirationManager;
   private final CacheNotifier<K, V> notifier;

   public SimpleReadWriteMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
      this.expirationManager = ComponentRegistry.componentOf(functionalMap.cache, InternalExpirationManager.class);
      this.notifier = ComponentRegistry.componentOf(functionalMap.cache, CacheNotifier.class);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      return eval(key, null, (ignore, v) -> f.apply(v));
   }

   @Override
   public <T, R> CompletableFuture<R> eval(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f) {
      K storageKey = toStorageKey(key);
      InternalCacheEntry<K, V> ice = fmap.cache.getDataContainer().peek(storageKey);
      boolean expired = false;
      if (ice != null && ice.canExpire()) {
         int segment = fmap.keyPartitioner.getSegment(storageKey);
         CompletionStage<Boolean> expiration = expirationManager.handlePossibleExpiration(ice, segment, true);
         expired = toLocalExecution(expiration.toCompletableFuture());

         if (expired && notifier.hasListener(CacheEntryExpired.class)) {
            CompletionStages.join(notifier.notifyCacheEntryExpired(ice.getKey(), ice.getValue(), ice.getMetadata(), ImmutableContext.INSTANCE));
         }
      }

      try {
         InternalCacheEntry<K, V> entry = !expired ? ice : null;
         R result = evalInternal(storageKey, argument, f, entry);
         return result == null
               ? CompletableFutures.completedNull()
               : CompletableFuture.completedFuture(result);
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public <T, R> Traversable<R> evalMany(Map<? extends K, ? extends T> arguments, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f) {
      // Since we're in a simple cache, everything is local!
      List<R> results = new ArrayList<>(arguments.size());
      for (Map.Entry<? extends K, ? extends T> me : arguments.entrySet()) {
         CompletableFuture<R> cf = eval(me.getKey(), me.getValue(), f);
         results.add(toLocalExecution(cf));
      }

      return Traversables.of(results.stream());
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      List<R> results = new ArrayList<>(keys.size());
      BiFunction<?, EntryView.ReadWriteEntryView<K, V>, R> bf = (ignore, v) -> f.apply(v);
      for (K key : keys) {
         results.add(toLocalExecution(eval(key, null, bf)));
      }

      return Traversables.of(results.stream());
   }

   @Override
   public <R> Traversable<R> evalAll(Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      return evalMany(fmap.cache.keySet(), f);
   }

   private <R, T> R evalInternal(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f, InternalCacheEntry<K, V> override) {
      Objects.requireNonNull(key, KEY_CANNOT_BE_NULL);
      Objects.requireNonNull(f, FUNCTION_CANNOT_BE_NULL);
      ByRef<R> result = new ByRef<>(null);
      ByRef<SimpleWriteNotifierHelper.EntryHolder<K, V>> holder = new ByRef<>(null);
      fmap.cache.getDataContainer().compute(key, (k, ignore, factory) ->
            compute(key, argument, f, override, factory, result, holder));
      SimpleWriteNotifierHelper.handleNotification(notifier, fmap.notifier, key, holder.get(), false);
      return result.get();
   }

   private <R, T> InternalCacheEntry<K, V> compute(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f,
                                                   InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory,
                                                   ByRef<R> result, ByRef<SimpleWriteNotifierHelper.EntryHolder<K, V>> holder) {
      MVCCEntry<K, V> e = readCacheEntry(key, oldEntry);
      EntryViews.AccessLoggingReadWriteView<K, V> view = EntryViews.readWrite(e, fmap.cache.getKeyDataConversion(), fmap.cache.getValueDataConversion());
      result.set(EntryViews.snapshot(f.apply(argument, view)));

      if (!e.isChanged()) return oldEntry;

      SimpleWriteNotifierHelper.EntryHolder<K, V> eh = oldEntry == null
            ? SimpleWriteNotifierHelper.create(null, e)
            : SimpleWriteNotifierHelper.create(factory.copy(oldEntry), e);
      holder.set(eh);
      SimpleWriteNotifierHelper.handleNotification(notifier, fmap.notifier, key, eh, true);

      if (e.isRemoved()) return null;

      return oldEntry == null
            ? factory.create(e)
            : factory.update(oldEntry, e.getValue(), e.getMetadata());
   }
}
