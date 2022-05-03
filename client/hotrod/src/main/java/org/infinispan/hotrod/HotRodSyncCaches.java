package org.infinispan.hotrod;

import static org.infinispan.hotrod.impl.Util.await;

import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.SyncCaches;
import org.infinispan.hotrod.impl.cache.RemoteCache;

/**
 * @since 14.0
 **/
public class HotRodSyncCaches implements SyncCaches {
   private final HotRod hotrod;

   public HotRodSyncCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> HotRodSyncCache<K, V> get(String name) {
      //FIXME
      return await(hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodSyncCache<>(hotrod, (RemoteCache<K, V>) r)));
   }

   @Override
   public <K, V> HotRodSyncCache<K, V> create(String name, CacheConfiguration cacheConfiguration) {
      //FIXME
      return await(hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodSyncCache<>(hotrod, (RemoteCache<K, V>) r)));
   }

   @Override
   public <K, V> HotRodSyncCache<K, V> create(String name, String template) {
      return await(hotrod.transport.getRemoteCache(name).thenApply(r -> new HotRodSyncCache<>(hotrod, (RemoteCache<K, V>) r)));
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public Iterable<String> names() {
      return null;
   }

   @Override
   public void createTemplate(String name, CacheConfiguration cacheConfiguration) {

   }

   @Override
   public void removeTemplate(String name) {

   }

   @Override
   public Iterable<String> templateNames() {
      return null;
   }
}
