package org.infinispan.functional.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.functional.EntryView;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.security.actions.SecurityActions;

/**
 * A {@link WriteOnlyMapImpl} that works with a simple cache.
 *
 * @since 15.0
 * @see WriteOnlyMapImpl
 */
@Experimental
public class SimpleWriteOnlyMapImpl<K, V> extends WriteOnlyMapImpl<K, V> implements SimpleFunctionalMap<K, V> {

   private final CacheNotifier<K, V> notifier;

   protected SimpleWriteOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
      this.notifier = SecurityActions.getCacheComponentRegistry(functionalMap.cache)
            .getComponent(CacheNotifier.class);
   }

   public static <K, V> WriteOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> WriteOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      return new SimpleWriteOnlyMapImpl<>(params, functionalMap);
   }


   @Override
   public CompletableFuture<Void> eval(K key, Consumer<EntryView.WriteEntryView<K, V>> f) {
      return eval(key, null, (ignore, e) -> f.accept(e));
   }

   @Override
   public <T> CompletableFuture<Void> eval(K key, T argument, BiConsumer<T, EntryView.WriteEntryView<K, V>> f) {
      K storageKey = toStorageKey(key);

      ByRef<SimpleWriteNotifierHelper.EntryHolder<K, V>> holder = new ByRef<>(null);
      fmap.cache.getDataContainer().compute(storageKey, (ignore, entry, factory) -> {
         MVCCEntry<K, V> e = readCacheEntry(storageKey, entry);
         f.accept(argument, EntryViews.writeOnly(e, fmap.cache.getValueDataConversion()));

         if (!e.isChanged()) return entry;

         SimpleWriteNotifierHelper.EntryHolder<K, V> eh = entry == null
               ? SimpleWriteNotifierHelper.createWriteOnly(null, e)
               : SimpleWriteNotifierHelper.createWriteOnly(factory.copy(entry), e);

         holder.set(eh);
         SimpleWriteNotifierHelper.handleNotification(notifier, fmap.notifier, key, eh, true);

         if (e.isRemoved()) return null;

         return entry == null
               ? factory.create(e)
               : factory.update(entry, e.getValue(), e.getMetadata());
      });

      SimpleWriteNotifierHelper.handleNotification(notifier, fmap.notifier, key, holder.get(), false);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> evalMany(Set<? extends K> keys, Consumer<EntryView.WriteEntryView<K, V>> f) {
      for (K key : keys) {
         toLocalExecution(eval(key, f));
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public <T> CompletableFuture<Void> evalMany(Map<? extends K, ? extends T> arguments, BiConsumer<T, EntryView.WriteEntryView<K, V>> f) {
      for (Map.Entry<? extends K, ? extends T> entry : arguments.entrySet()) {
         toLocalExecution(eval(entry.getKey(), entry.getValue(), f));
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> evalAll(Consumer<EntryView.WriteEntryView<K, V>> f) {
      return evalMany(fmap.cache.keySet(), f);
   }
}
