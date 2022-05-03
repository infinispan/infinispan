package org.infinispan.hotrod.multimap;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.common.CacheEntryCollection;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;

/**
 * @param <K>
 * @param <V>
 * @since 14.0
 */
public interface RemoteMultimapCache<K, V> {

   CompletionStage<CacheEntryCollection<K, V>> getWithMetadata(K key, CacheOptions options);

   CompletionStage<Void> put(K key, V value, CacheWriteOptions options);

   CompletionStage<Collection<V>> get(K key, CacheOptions options);

   CompletionStage<Boolean> remove(K key, CacheOptions options);

   CompletionStage<Boolean> remove(K key, V value, CacheOptions options);

   CompletionStage<Boolean> containsKey(K key, CacheOptions options);

   CompletionStage<Boolean> containsValue(V value, CacheOptions options);

   CompletionStage<Boolean> containsEntry(K key, V value, CacheOptions options);

   CompletionStage<Long> size(CacheOptions options);

   boolean supportsDuplicates();
}
