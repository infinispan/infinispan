package org.infinispan.hotrod;

import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.mutiny.MutinyMultimap;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyMultimap<K, V> implements MutinyMultimap<K, V> {
   private final HotRod hotrod;
   private final String name;

   HotRodMutinyMultimap(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public Uni<MultimapConfiguration> configuration() {
      return null;
   }

   @Override
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<Void> add(K key, V value) {
      return null;
   }

   @Override
   public Multi<V> get(K key) {
      return null;
   }

   @Override
   public Uni<Boolean> remove(K key) {
      return null;
   }

   @Override
   public Uni<Boolean> remove(K key, V value) {
      return null;
   }

   @Override
   public Uni<Boolean> containsKey(K key) {
      return null;
   }

   @Override
   public Uni<Boolean> containsEntry(K key, V value) {
      return null;
   }

   @Override
   public Uni<Long> estimateSize() {
      return null;
   }
}
