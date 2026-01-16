package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.impl.function.multimap.ContainsFunction;
import org.infinispan.multimap.impl.function.multimap.GetFunction;
import org.infinispan.multimap.impl.function.multimap.PutFunction;
import org.infinispan.multimap.impl.function.multimap.RemoveFunction;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializablePredicate;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Embedded implementation of {@link MultimapCache}
 *
 * <h2>Transactions</h2>
 *
 * EmbeddedMultimapCache supports implicit transactions without blocking. The following methods block when
 * they are called in a explicit transaction context. This limitation could be improved in the following versions if
 * technically possible :
 *
 * <ul>
 *    <li>{@link org.infinispan.commons.api.multimap.BasicMultimapCache#size()} </li>
 *    <li>{@link org.infinispan.commons.api.multimap.BasicMultimapCache#containsEntry(Object, Object)}</li>
 * </ul>
 *
 * More about transactions in :
 * <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#transactions">the Infinispan Documentation</a>.
 *
 * <h2>Duplicates</h2>
 * MultimapCache can optionally support duplicate values on keys.
 *
 * <pre>
 *    multimapCache.put("k", "v1").join();
 *    multimapCache.put("k", "v2").join();
 *    multimapCache.put("k", "v2").join();
 *    multimapCache.put("k", "v2").join();
 *
 *    multimapCache.get("k").thenAccept(values -> System.out.println(values.size()));
 *    // prints the value 4. "k" -> ["v1", "v2", "v2", "v2"]
 * </pre>
 *
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class EmbeddedMultimapCache<K, V> implements MultimapCache<K, V> {

   private final FunctionalMap.ReadWriteMap<K, Bucket<V>> readWriteMap;
   private final AdvancedCache<K, Bucket<V>> cache;
   private final InternalEntryFactory entryFactory;
   private final boolean supportsDuplicates;

   public EmbeddedMultimapCache(Cache<K, Bucket<V>> cache, boolean supportsDuplicates) {
      //TODO: ISPN-11452 Multimaps don't support transcoding, so disable data conversions
      this.cache = cache.getAdvancedCache();
      FunctionalMap<K, Bucket<V>> functionalMap = FunctionalMap.create(this.cache);
      this.readWriteMap = functionalMap.toReadWriteMap();
      this.entryFactory = ComponentRegistry.of(this.cache).getInternalEntryFactory().running();
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public CompletableFuture<Void> put(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new PutFunction<>(value, supportsDuplicates));
   }

   @Override
   public CompletableFuture<Collection<V>> get(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new GetFunction<>(supportsDuplicates));
   }

   @Override
   public CompletableFuture<Optional<CacheEntry<K, Collection<V>>>> getEntry(K key) {
      requireNonNull(key, "key can't be null");
      return cache.getAdvancedCache().getCacheEntryAsync(key)
           .thenApply(entry -> {
              if (entry == null)
                 return Optional.empty();

              return Optional.of(entryFactory.create(entry.getKey(),(supportsDuplicates ? entry.getValue().toList(): entry.getValue().toSet()) , entry.getMetadata()));
           });
   }

   @Override
   public CompletableFuture<Boolean> remove(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new RemoveFunction<>());
   }

   @Override
   public CompletableFuture<Boolean> remove(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new RemoveFunction<>(value, supportsDuplicates));
   }

   @Override
   public CompletableFuture<Void> remove(Predicate<? super V> p) {
      requireNonNull(p, "predicate can't be null");
      return this.removeInternal(p).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>());
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      requireNonNull(value, "value can't be null");
      return containsEntryInternal(value).toCompletableFuture();
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>(value));
   }

   @Override
   public CompletableFuture<Long> size() {
      return sizeInternal().toCompletableFuture();
   }

   private CompletionStage<Void> removeInternal(Predicate<? super V> p) {

      Publisher<K> keyPublisher = cache.cachePublisher().entryPublisher(publisher ->
            Flowable.fromPublisher(publisher)
                  .filter(ce -> {
                     for (V v : ce.getValue().values) {
                        if (p.test(v)) {
                           return true;
                        }
                     }
                     return false;
                  }).map(CacheEntry::getKey)
      ).publisherWithoutSegments();

      return Flowable.fromPublisher(keyPublisher)
            .concatMapCompletable(key -> Completable.fromCompletionStage(cache.computeIfPresentAsync(key, (k, bucket) -> {
                     Bucket<V> newBucket = bucket.removeIf(p);
                     if (newBucket == null) {
                        return bucket;
                     }
                     return newBucket.isEmpty() ? null : newBucket;
                  }
            ))).toCompletionStage(null);
   }

   private CompletionStage<Boolean> containsEntryInternal(V value) {
      SerializablePredicate<CacheEntry<K, Bucket<V>>> func = entry -> entry.getValue().contains(value);
      return cache.cachePublisher()
            // We can relax guarantee since we don't care if the value was found more than once or not
            .atLeastOnce()
            .entryReduction(PublisherReducers.anyMatch(func), PublisherReducers.or());
   }

   private CompletionStage<Long> sizeInternal() {
      SerializableBiFunction<Long, CacheEntry<K, Bucket<V>>, Long> func =
            (sum, entry) -> sum + entry.getValue().size();
      return cache.cachePublisher()
            .entryReduction(PublisherReducers.reduce((long) 0, func), PublisherReducers.add());
   }

   @Override
   public boolean supportsDuplicates() {
      return supportsDuplicates;
   }

   public Cache<K, Bucket<V>> getCache() {
      return cache;
   }
}
