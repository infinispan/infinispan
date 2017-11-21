package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.infinispan.util.concurrent.CompletableFutures.rethrowException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.api.BasicMultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.impl.function.ContainsFunction;
import org.infinispan.multimap.impl.function.GetFunction;
import org.infinispan.multimap.impl.function.PutFunction;
import org.infinispan.multimap.impl.function.RemoveFunction;
import org.infinispan.util.concurrent.CompletableFutures;

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
 * The current implementation does not support duplicate values on keys. {@link
 * Object#equals(Object)} method is used to check if a value is already present in the key. This means that the
 * following code.
 *
 * <pre>
 *    multimapCache.put("k", "v1").join();
 *    multimapCache.put("k", "v2").join();
 *    multimapCache.put("k", "v2").join();
 *    multimapCache.put("k", "v2").join();
 *
 *    multimapCache.get("k").thenAccept(values -> System.out.println(values.size()));
 *    // prints the value 2. "k" -> ["v1", "v2"]
 * </pre>
 *
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class EmbeddedMultimapCache<K, V> implements MultimapCache<K, V> {

   private FunctionalMap.ReadWriteMap<K, Collection<V>> readWriteMap;
   private Cache<K, Collection<V>> cache;

   public EmbeddedMultimapCache(Cache<K, Collection<V>> cache) {
      this.cache = cache;
      FunctionalMapImpl<K, Collection<V>> functionalMap = FunctionalMapImpl.create(this.cache.getAdvancedCache());
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
   }

   @Override
   public CompletableFuture<Void> put(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new PutFunction<>(value));
   }

   @Override
   public CompletableFuture<Collection<V>> get(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new GetFunction<>());
   }

   @Override
   public CompletableFuture<Optional<CacheEntry<K, Collection<V>>>> getEntry(K key) {
      requireNonNull(key, "key can't be null");
      return CompletableFuture.supplyAsync(() -> Optional.ofNullable(cache.getAdvancedCache().getCacheEntry(key)));
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
      return readWriteMap.eval(key, new RemoveFunction<>(value));
   }

   @Override
   public CompletableFuture<Void> remove(Predicate<? super V> p) {
      requireNonNull(p, "predicate can't be null");
      CompletableFuture<Void> cf = CompletableFutures.completedNull();
      try {
         if (isExplicitTxContext()) {
            // block on explicit tx
            cf = completedFuture(this.removeInternal(p));
         } else {
            cf = runAsync(() -> this.removeInternal(p));
         }
      } catch (SystemException e) {
         rethrowException(e);
      }
      return cf;
   }

   @Override
   public CompletableFuture<Boolean> containsKey(K key) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>());
   }

   @Override
   public CompletableFuture<Boolean> containsValue(V value) {
      requireNonNull(value, "value can't be null");
      CompletableFuture<Boolean> cf = CompletableFutures.completedNull();
      try {
         if (isExplicitTxContext()) {
            // block on explicit tx
            cf = completedFuture(containsEntryInternal(value));
         } else {
            cf = supplyAsync(() -> containsEntryInternal(value));
         }
      } catch (SystemException e) {
         rethrowException(e);
      }
      return cf;
   }

   @Override
   public CompletableFuture<Boolean> containsEntry(K key, V value) {
      requireNonNull(key, "key can't be null");
      requireNonNull(value, "value can't be null");
      return readWriteMap.eval(key, new ContainsFunction<>(value));
   }

   @Override
   public CompletableFuture<Long> size() {
      CompletableFuture<Long> cf = CompletableFutures.completedNull();
      try {
         if (isExplicitTxContext()) {
            // block on explicit tx
            cf = completedFuture(sizeInternal());
         } else {
            cf = supplyAsync(() -> sizeInternal());
         }
      } catch (SystemException e) {
         rethrowException(e);
      }
      return cf;
   }

   private boolean isExplicitTxContext() throws SystemException {
      TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
      return transactionManager != null && transactionManager.getTransaction() != null;
   }

   private Void removeInternal(Predicate<? super V> p) {
      cache.keySet().stream().forEach((c, key) -> c.computeIfPresent(key, (o, o1) -> {
         Collection<V> values = (Collection<V>) o1;
         Collection<V> newValues = new HashSet<>();
         for (V v : values) {
            if (!p.test(v))
               newValues.add(v);
         }
         return newValues.isEmpty() ? null : newValues;
      }));
      return null;
   }

   private Boolean containsEntryInternal(V value) {
      return cache.entrySet().parallelStream().anyMatch(entry -> entry.getValue().contains(value));
   }

   private Long sizeInternal() {
      return cache.values().parallelStream().mapToLong(value -> value.size()).sum();
   }

   @Override
   public boolean supportsDuplicates() {
      return false;
   }

   public Cache<K, Collection<V>> getCache() {
      return cache;
   }
}
