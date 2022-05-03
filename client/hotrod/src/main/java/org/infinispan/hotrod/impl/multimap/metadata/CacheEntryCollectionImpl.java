package org.infinispan.hotrod.impl.multimap.metadata;

import java.util.Collection;
import java.util.Objects;

import org.infinispan.api.common.CacheEntryCollection;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;

/**
 * The values used in this class are assumed to be in MILLISECONDS
 *
 * @since 14.0
 */
public class CacheEntryCollectionImpl<K, V> implements CacheEntryCollection<K, V> {
   private final K key;
   private final Collection<V> collection;
   private CacheEntryMetadata metadata;

   public CacheEntryCollectionImpl(K key, Collection<V> collection) {
      this(key, collection, new CacheEntryMetadataImpl());
   }

   public CacheEntryCollectionImpl(K key, Collection<V> collection, CacheEntryMetadata metadata) {
      this.key = key;
      this.collection = collection;
      this.metadata = metadata;
   }

   @Override
   public K key() {
      return key;
   }

   @Override
   public Collection<V> values() {
      return collection;
   }


   @Override
   public CacheEntryMetadata metadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheEntryCollectionImpl<?, ?> that = (CacheEntryCollectionImpl<?, ?>) o;
      return key.equals(that.key) && Objects.equals(collection, that.collection) && Objects.equals(metadata, that.metadata);
   }

   @Override
   public int hashCode() {
      return Objects.hash(key, collection, metadata);
   }

   @Override
   public String toString() {
      return "CacheEntryCollectionImpl{" +
            "key=" + key +
            ", collection=" + collection +
            ", metadata=" + metadata +
            '}';
   }
}
