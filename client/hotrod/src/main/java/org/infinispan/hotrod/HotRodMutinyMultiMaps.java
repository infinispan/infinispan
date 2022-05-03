package org.infinispan.hotrod;

import org.infinispan.api.configuration.MultiMapConfiguration;
import org.infinispan.api.mutiny.MutinyMultiMap;
import org.infinispan.api.mutiny.MutinyMultiMaps;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyMultiMaps implements MutinyMultiMaps {
   private final HotRod hotrod;

   HotRodMutinyMultiMaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> Uni<MutinyMultiMap<K, V>> create(String name, MultiMapConfiguration cacheConfiguration) {
      return Uni.createFrom().item(new HotRodMutinyMultiMap(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public <K, V> Uni<MutinyMultiMap<K, V>> create(String name, String template) {
      return Uni.createFrom().item(new HotRodMutinyMultiMap(hotrod, name)); // PLACEHOLDER
   }

   @Override
   public <K, V> Uni<MutinyMultiMap<K, V>> get(String name) {
      return Uni.createFrom().item(new HotRodMutinyMultiMap(hotrod, name)); // PLACEHOLDER
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
   public Uni<Void> createTemplate(String name, MultiMapConfiguration cacheConfiguration) {
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
