package org.infinispan.embedded;

import org.infinispan.Cache;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCaches;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyCaches implements MutinyCaches {
   private final Embedded embedded;

   EmbeddedMutinyCaches(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, CacheConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, String template) {
      return null;
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> get(String name) {
      Uni<Cache<K, V>> uni = Uni.createFrom().completionStage(embedded.cacheManager.getCacheAsync(name));
      return uni.onItem().transform(Cache::getAdvancedCache).onItem().transform(a -> new EmbeddedMutinyCache<>(embedded, a));
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
