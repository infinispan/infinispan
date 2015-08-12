package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.near.NearCacheService;

/**
 * Near {@link org.infinispan.client.hotrod.RemoteCache} implementation
 * enabling
 *
 * @param <K>
 * @param <V>
 */
@Deprecated
public class EagerNearRemoteCache<K, V> extends RemoteCacheImpl<K, V> {

   private final NearCacheService<K, V> nearcache;

   public EagerNearRemoteCache(RemoteCacheManager rcm, String name, NearCacheService<K, V> nearcache) {
      super(rcm, name);
      this.nearcache = nearcache;
   }

   @Override
   public V get(Object key) {
      VersionedValue<V> versioned = getVersioned((K) key);
      return versioned != null ? versioned.getValue() : null;
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      VersionedValue<V> nearValue = nearcache.get(key);
      if (nearValue == null) {
         VersionedValue<V> remoteValue = super.getVersioned(key);
         if (remoteValue != null)
            nearcache.putIfAbsent(key, remoteValue);

         return remoteValue;
      }

      return nearValue;
   }

   @Override
   public void start() {
      nearcache.start(this);
   }

   @Override
   public void stop() {
      nearcache.stop(this);
   }
}
