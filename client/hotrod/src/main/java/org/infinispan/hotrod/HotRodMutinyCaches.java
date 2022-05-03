package org.infinispan.hotrod;

import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCaches;
import org.infinispan.hotrod.impl.cache.RemoteCache;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyCaches implements MutinyCaches {
   private final HotRod hotrod;

   HotRodMutinyCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      // FIXME
      return Uni.createFrom().item(hotrod.transport.getRemoteCache(name)).map(r -> new HotRodMutinyCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, String template) {
      // FIXME
      return Uni.createFrom().item(hotrod.transport.getRemoteCache(name)).map(r -> new HotRodMutinyCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> get(String name) {
      return Uni.createFrom().item(hotrod.transport.getRemoteCache(name)).map(r -> new HotRodMutinyCache<>(hotrod, (RemoteCache<K, V>) r));
   }

   @Override
   public Uni<Void> remove(String name) {
      return null;
   }

   @Override
   public Multi<String> names() {
      return null;
   }

   @Override
   public Uni<Void> createTemplate(String name, CacheConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public Uni<Void> removeTemplate(String name) {
      return null;
   }

   @Override
   public Multi<String> templateNames() {
      return null;
   }
}
