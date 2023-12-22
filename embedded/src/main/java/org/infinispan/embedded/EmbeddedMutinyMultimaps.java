package org.infinispan.embedded;

import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.mutiny.MutinyMultimap;
import org.infinispan.api.mutiny.MutinyMultimaps;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyMultimaps implements MutinyMultimaps {
   private final Embedded embedded;

   EmbeddedMutinyMultimaps(Embedded embedded) {
      this.embedded = embedded;
   }

   @Override
   public <K, V> Uni<MutinyMultimap<K, V>> create(String name, MultimapConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public <K, V> Uni<MutinyMultimap<K, V>> create(String name, String template) {
      return null;
   }

   @Override
   public <K, V> Uni<MutinyMultimap<K, V>> get(String name) {
      return null;
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
   public Uni<Void> createTemplate(String name, MultimapConfiguration cacheConfiguration) {
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
