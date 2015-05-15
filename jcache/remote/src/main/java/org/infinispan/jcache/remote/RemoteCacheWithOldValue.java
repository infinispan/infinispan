package org.infinispan.jcache.remote;

import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.util.CloseableIterator;

/**
 * Wrapper which emulates replace and remove with oldValue.
 */
class RemoteCacheWithOldValue<K, V> extends RemoteCacheWrapper<K, V> {
   public RemoteCacheWithOldValue(RemoteCache<K, V> delegate) {
      super(delegate);
   }

   @Override
   public boolean remove(Object key, Object oldValue) {
      @SuppressWarnings("unchecked")
      K k = (K) key;

      VersionedValue<V> versioned = delegate.getVersioned(k);
      if (versioned == null) {
         return false;
      }
      if (!oldValue.equals(versioned.getValue())) {
         return false;
      }
      return delegate.removeWithVersion(k, versioned.getVersion());
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      VersionedValue<V> versioned = delegate.getVersioned(key);
      if (versioned == null) {
         return false;
      }
      if (!oldValue.equals(versioned.getValue())) {
         return false;
      }
      return delegate.replaceWithVersion(key, newValue, versioned.getVersion());
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, segments, batchSize);
   }
}
