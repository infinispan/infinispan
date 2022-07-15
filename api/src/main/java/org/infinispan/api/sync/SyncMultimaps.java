package org.infinispan.api.sync;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
public interface SyncMultimaps {
   <K, V> SyncMultimap<K, V> get(String name);

   <K, V> SyncMultimap<K, V> create(String name, MultimapConfiguration cacheConfiguration);

   <K, V> SyncMultimap<K, V> create(String name, String template);

   void remove(String name);

   CloseableIterable<String> names();

   void createTemplate(String name, MultimapConfiguration cacheConfiguration);

   void removeTemplate(String name);

   CloseableIterable<String> templateNames();
}
