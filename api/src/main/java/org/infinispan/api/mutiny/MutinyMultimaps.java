package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.MultimapConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyMultimaps {
   <K, V> Uni<MutinyMultimap<K, V>> create(String name, MultimapConfiguration cacheConfiguration);

   <K, V> Uni<MutinyMultimap<K, V>> create(String name, String template);

   <K, V> Uni<MutinyMultimap<K, V>> get(String name);

   Uni<Void> remove(String name);

   Multi<String> names();

   Uni<Void> createTemplate(String name, MultimapConfiguration cacheConfiguration);

   Uni<Void> removeTemplate(String name);

   Multi<String> templateNames();
}
