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
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.Param;
import org.infinispan.functional.Traversable;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * A {@link ReadWriteMapImpl} that works with a simple cache.
 *
 * @since 15.0
 * @author José Bolina
 * @see ReadWriteMapImpl
 */
@Experimental
public class SimpleReadWriteMapImpl<K, V> extends ReadWriteMapImpl<K, V> {
   private static final String KEY_CANNOT_BE_NULL = "Key cannot be null";
   private static final String FUNCTION_CANNOT_BE_NULL = "Function cannot be null";

   private final InternalExpirationManager<K, V> expirationManager;
   private final CacheNotifier<K, V> notifier;

   public SimpleReadWriteMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
      this.expirationManager = functionalMap.cache.getComponentRegistry().getComponent(InternalExpirationManager.class);
      this.notifier = functionalMap.cache.getComponentRegistry().getComponent(CacheNotifier.class);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      return eval(key, null, (ignore, v) -> f.apply(v));
   }

   @Override
   public <T, R> CompletableFuture<R> eval(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f) {
      K storageKey = storageKey(key);
      InternalCacheEntry<K, V> ice = fmap.cache.getDataContainer().peek(storageKey);
      boolean expired = false;
      if (ice != null && ice.canExpire()) {
         int segment = fmap.keyPartitioner.getSegment(storageKey);
         CompletionStage<Boolean> expiration = expirationManager.handlePossibleExpiration(ice, segment, true);
         assert expiration.toCompletableFuture().isDone() : "Expiration CF is not done!";
         expired = CompletionStages.join(expiration);

         if (expired && notifier.hasListener(CacheEntryExpired.class)) {
            CompletionStages.join(notifier.notifyCacheEntryExpired(ice.getKey(), ice.getValue(), ice.getMetadata(),   ImmutableContext.INSTANCE));
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
         assert cf.isDone() : "eval() returned an incomplete CompletableFuture";

         results.add(CompletionStages.join(cf));
      }

      return Traversables.of(results.stream());
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      List<R> results = new ArrayList<>(keys.size());
      BiFunction<?, EntryView.ReadWriteEntryView<K, V>, R> bf = (ignore, v) -> f.apply(v);
      for (K key : keys) {
         CompletableFuture<R> cf = eval(key, null, bf);
         assert cf.isDone() : "eval() returned an incomplete CompletableFuture";

         results.add(CompletionStages.join(cf));
      }

      return Traversables.of(results.stream());
   }

   @Override
   public <R> Traversable<R> evalAll(Function<EntryView.ReadWriteEntryView<K, V>, R> f) {
      return evalMany(fmap.cache.keySet(), f);
   }

   private boolean isStatisticsEnabled() {
      return !params.containsAll(Param.StatisticsMode.SKIP);
   }

   private boolean hasListeners() {
      return !notifier.getListeners().isEmpty();
   }

   private <R, T> R evalInternal(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f, InternalCacheEntry<K, V> override) {
      Objects.requireNonNull(key, KEY_CANNOT_BE_NULL);
      Objects.requireNonNull(f, FUNCTION_CANNOT_BE_NULL);
      ByRef<R> result = new ByRef<>(null);
      ByRef<EntryHolder<K, V>> holder = new ByRef<>(null);
      fmap.cache.getDataContainer().compute(key, (k, ignore, factory) ->
            compute(key, argument, f, override, factory, result, holder));
      handleNotifications(key, holder.get());
      return result.get();
   }

   private void handleNotifications(K key, EntryHolder<K, V> holder) {
      if (holder == null) return;

      InternalCacheEntry<K, V> oldEntry = holder.before;
      MVCCEntry<K, V> e = holder.after;

      if (hasListeners()) {
         if (oldEntry == null) {
            CompletionStages.join(notifier.notifyCacheEntryCreated(key, e.getValue(), e.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
         } else {
            CompletionStages.join(notifier.notifyCacheEntryModified(key, e.getValue(), e.getMetadata(), oldEntry.getValue(), oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null));
         }
      }
   }

   private <R, T> InternalCacheEntry<K, V> compute(K key, T argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f,
                                                   InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory,
                                                   ByRef<R> result, ByRef<EntryHolder<K, V>> holder) {
      MVCCEntry<K, V> e = extractEntry(oldEntry, key);
      EntryViews.AccessLoggingReadWriteView<K, V> view =  EntryViews.readWrite(e, fmap.cache.getKeyDataConversion(), fmap.cache.getValueDataConversion());
      result.set(EntryViews.snapshot(f.apply(argument, view)));

      if (!e.isChanged()) return oldEntry;

      if (hasListeners()) {
         if (oldEntry == null) {
            holder.set(new EntryHolder<>(null, e));
            CompletionStages.join(notifier.notifyCacheEntryCreated(key, e.getValue(), e.getOldMetadata(), true, ImmutableContext.INSTANCE, null));
         } else {
            holder.set(new EntryHolder<>(oldEntry, e));
            CompletionStages.join(notifier.notifyCacheEntryModified(key, e.getValue(), e.getMetadata(), oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
         }
      }

      return oldEntry == null
            ? factory.create(e)
            : factory.update(oldEntry, e.getValue(), e.getMetadata());
   }

   @SuppressWarnings("unchecked")
   private K storageKey(K key) {
      return (K) fmap.cache.getKeyDataConversion().toStorage(key);
   }

   private MVCCEntry<K, V> extractEntry(InternalCacheEntry<K, V> ice, K key) {
      if (ice == null) {
         return new ReadCommittedEntry<>(key, null, null);
      }

      return createWrapped(key, ice);
   }

   private MVCCEntry<K, V> createWrapped(K key, InternalCacheEntry<K, V> ice) {
      V value;
      Metadata metadata;
      PrivateMetadata internalMetadata;
      synchronized (ice) {
         value = ice.getValue();
         metadata = ice.getMetadata();
         internalMetadata = ice.getInternalMetadata();
      }
      MVCCEntry<K, V> mvccEntry = new ReadCommittedEntry<>(key, value, metadata);
      mvccEntry.setInternalMetadata(internalMetadata);
      mvccEntry.setCreated(ice.getCreated());
      mvccEntry.setLastUsed(ice.getLastUsed());
      return mvccEntry;
   }

   private static class EntryHolder<K, V> {
      private final InternalCacheEntry<K, V> before;
      private final MVCCEntry<K, V> after;

      private EntryHolder(InternalCacheEntry<K, V> before, MVCCEntry<K, V> after) {
         this.before = before;
         this.after = after;
      }
   }
}
