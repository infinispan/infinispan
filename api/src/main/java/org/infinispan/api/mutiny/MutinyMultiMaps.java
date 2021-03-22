package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.MultiMapConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyMultiMaps {
   <K, V> Uni<MutinyMultiMap<K, V>> create(String name, MultiMapConfiguration cacheConfiguration);

   <K, V> Uni<MutinyMultiMap<K, V>> create(String name, String template);

   <K, V> Uni<MutinyMultiMap<K, V>> get(String name);

   Uni<Void> remove(String name);

   Multi<String> names();

   <K, V> Uni<Void> createTemplate(String name, MultiMapConfiguration cacheConfiguration);

   Uni<Void> removeTemplate(String name);

   Multi<String> templateNames();
}
