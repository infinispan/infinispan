package org.infinispan.embedded;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncMultimap;
import org.infinispan.api.sync.SyncMultimaps;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;

/**
 * @since 15.0
 */
public class EmbeddedSyncMultimaps implements SyncMultimaps {
   private final Embedded embedded;
   private final MultimapCacheManager multimapCacheManager;

   EmbeddedSyncMultimaps(Embedded embedded) {
      this.embedded = embedded;
      this.multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(embedded.cacheManager);
   }

   @Override
   public <K, V> SyncMultimap<K, V> get(String name) {
      MultimapCache<K, V> multimapCache = multimapCacheManager.get(name);
      return new EmbeddedSyncMultimap(embedded, multimapCache);
   }

   @Override
   public <K, V> SyncMultimap<K, V> create(String name, MultimapConfiguration cacheConfiguration) {
      return null;
   }

   @Override
   public <K, V> SyncMultimap<K, V> create(String name, String template) {
      return null;
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public CloseableIterable<String> names() {
      return null;
   }

   @Override
   public void createTemplate(String name, MultimapConfiguration cacheConfiguration) {

   }

   @Override
   public void removeTemplate(String name) {

   }

   @Override
   public CloseableIterable<String> templateNames() {
      return null;
   }
}
