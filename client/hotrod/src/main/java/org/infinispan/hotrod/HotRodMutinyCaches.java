package org.infinispan.hotrod;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCaches;

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
      return Uni.createFrom().completionStage(() -> hotrod.transport.<K, V>getRemoteCache(name))
            .map(r -> new HotRodMutinyCache<>(hotrod, r));
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> create(String name, String template) {
      return Uni.createFrom().completionStage(() -> hotrod.transport.<K, V>getRemoteCache(name))
            .map(r -> new HotRodMutinyCache<>(hotrod, r));
   }

   @Override
   public <K, V> Uni<MutinyCache<K, V>> get(String name) {
      return Uni.createFrom().completionStage(() -> hotrod.transport.<K, V>getRemoteCache(name))
            .map(r -> new HotRodMutinyCache<>(hotrod, r));
   }

   @Override
   public Uni<Void> remove(String name) {
      return Uni.createFrom().completionStage(() -> hotrod.transport.removeCache(name));
   }

   @Override
   public Multi<String> names() {
      return Multi.createFrom().deferred(() -> toMulti(hotrod.transport.getCacheNames()));
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
      return Multi.createFrom().deferred(() -> toMulti(hotrod.transport.getTemplateNames()));
   }

   private Multi<String> toMulti(CompletionStage<Set<String>> cs) {
      return Multi.createFrom().completionStage(cs).onItem().transformToIterable(Function.identity());
   }
}
