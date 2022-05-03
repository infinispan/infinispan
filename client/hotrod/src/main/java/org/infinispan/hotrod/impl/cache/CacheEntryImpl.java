package org.infinispan.hotrod.impl.cache;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryMetadata;

/**
 * @since 14.0
 **/
public class CacheEntryImpl<K, V> implements CacheEntry<K, V> {
   private final K k;
   private final V v;
   private final CacheEntryMetadata metadata;

   public CacheEntryImpl(K k, V v, CacheEntryMetadata metadata) {
      this.k = k;
      this.v = v;
      this.metadata = metadata;
   }

   @Override
   public K key() {
      return k;
   }

   @Override
   public V value() {
      return v;
   }

   @Override
   public CacheEntryMetadata metadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheEntryImpl<?, ?> that = (CacheEntryImpl<?, ?>) o;
      return k.equals(that.k) && Objects.equals(v, that.v) && Objects.equals(metadata, that.metadata);
   }

   @Override
   public int hashCode() {
      return Objects.hash(k, v, metadata);
   }

   @Override
   public String toString() {
      return "CacheEntryImpl{" +
            "k=" + toStr(k) +
            ", v=" + toStr(v) +
            ", metadata=" + metadata +
            '}';
   }
}
