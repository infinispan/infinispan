package org.infinispan.api.sync;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultiMapConfiguration;

/**
 * @since 14.0
 **/
public interface SyncMultiMaps {
   <K, V> SyncCache<K, V> get(String name);

   <K, V> SyncCache<K, V> create(String name, MultiMapConfiguration cacheConfiguration);

   <K, V> SyncCache<K, V> create(String name, String template);

   void remove(String name);

   CloseableIterable<String> names();

   void createTemplate(String name, MultiMapConfiguration cacheConfiguration);

   void removeTemplate(String name);

   CloseableIterable<String> templateNames();
}
