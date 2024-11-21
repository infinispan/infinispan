package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncMultimap;
import org.infinispan.api.sync.SyncMultimaps;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncMultimaps implements SyncMultimaps {
   private final HotRod hotrod;

   HotRodSyncMultimaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> SyncMultimap<K, V> get(String name) {
      return new HotRodSyncMultimap<>(hotrod, name);
   }

   @Override
   public <K, V> SyncMultimap<K, V> create(String name, MultimapConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <K, V> SyncMultimap<K, V> create(String name, String template) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void remove(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CloseableIterable<String> names() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void createTemplate(String name, MultimapConfiguration cacheConfiguration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeTemplate(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CloseableIterable<String> templateNames() {
      throw new UnsupportedOperationException();
   }
}
