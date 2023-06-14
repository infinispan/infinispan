package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.api.BasicMultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.impl.function.multimap.ContainsFunction;
import org.infinispan.multimap.impl.function.multimap.GetFunction;
import org.infinispan.multimap.impl.function.multimap.PutFunction;
import org.infinispan.multimap.impl.function.multimap.RemoveFunction;
import org.infinispan.commons.util.concurrent.CompletableFutures;

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
 *    <li>{@link BasicMultimapCache#size()} </li>
 *    <li>{@link BasicMultimapCache#containsEntry(Object, Object)}</li>
 * </ul>
 *
 * More about transactions in :
 * <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#transactions">the Infinispan Documentation</a>.
 *
 * <h2>Duplicates</h2>
 * MultimapCache can optionally support duplicate values on keys. {@link
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
      FunctionalMapImpl<K, Bucket<V>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = this.cache.getComponentRegistry().getInternalEntryFactory().running();
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
      try {
         // block on explicit tx
         return isExplicitTxContext() ?
              completedFuture(this.removeInternal(p)) :
              runAsync(() -> this.removeInternal(p));
      } catch (SystemException e) {
         throw CompletableFutures.asCompletionException(e);
      }
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>());
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      requireNonNull(value, "value can't be null");
      try {
         // block on explicit tx
         return isExplicitTxContext() ?
              completedFuture(containsEntryInternal(value)) :
              supplyAsync(() -> containsEntryInternal(value));
      } catch (SystemException e) {
         throw CompletableFutures.asCompletionException(e);
      }
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>(value));
   }

   @Override
   public CompletableFuture<Long> size() {
      try {
         // block on explicit tx
         return isExplicitTxContext() ?
              completedFuture(sizeInternal()) :
              supplyAsync(this::sizeInternal);
      } catch (SystemException e) {
         throw CompletableFutures.asCompletionException(e);
      }
   }

   private boolean isExplicitTxContext() throws SystemException {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      return transactionManager != null && transactionManager.getTransaction() != null;
   }

   private Void removeInternal(Predicate<? super V> p) {
      // Iterate over keys on the caller thread so that compute operations join the running transaction (if any)
      cache.keySet().forEach(key -> cache.computeIfPresent(key, (k, bucket) -> {
         Bucket<V> newBucket = bucket.removeIf(p);
         if (newBucket == null) {
            return bucket;
         }
         return newBucket.isEmpty() ? null : newBucket;
      }));
      return null;
   }

   private Boolean containsEntryInternal(V value) {
      return cache.values().stream().anyMatch(bucket -> bucket.contains(value));
   }

   private Long sizeInternal() {
      return cache.values().stream().mapToLong(Bucket::size).sum();
   }

   @Override
   public boolean supportsDuplicates() {
      return supportsDuplicates;
   }

   public Cache<K, Bucket<V>> getCache() {
      return cache;
   }
}
