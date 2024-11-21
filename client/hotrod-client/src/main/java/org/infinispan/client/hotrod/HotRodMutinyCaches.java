package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCaches;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodMutinyCaches implements MutinyCaches {
   private final HotRod hotrod;

   HotRodMutinyCaches(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> get(String name) {
      InternalRemoteCache<K, V> cache = getCache(name);
      return Uni.createFrom().item(new HotRodMutinyCache<>(hotrod, cache));
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      hotrod.cacheManager.getConfiguration().addRemoteCache(name, builder -> builder.configuration(cacheConfiguration.toString()));
      return get(name);
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, String template) {
      hotrod.cacheManager.getConfiguration().addRemoteCache(name, builder -> builder.templateName(template));
      return get(name);
   }

   @Override
   public Uni<Void> remove(String name) {
      hotrod.cacheManager.administration().removeCache(name);
      return Uni.createFrom().voidItem();
   }

   @Override
   public Multi<String> names() {
      return Multi.createFrom()
            .deferred(() -> Multi.createFrom().iterable(hotrod.cacheManager.getCacheNames()));
   }

   @Override
   public Uni<Void> createTemplate(String name, CacheConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> removeTemplate(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<String> templateNames() {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   private <K, V> InternalRemoteCache<K, V> getCache(String name) {
      return (InternalRemoteCache<K, V>) hotrod.cacheManager.getCache(name);
   }
}
