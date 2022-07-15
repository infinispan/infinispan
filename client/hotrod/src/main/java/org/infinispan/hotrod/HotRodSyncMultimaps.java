package org.infinispan.hotrod;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncMultimaps;

/**
 * @since 14.0
 **/
public class HotRodSyncMultimaps implements SyncMultimaps {
   private final HotRod hotrod;

   public HotRodSyncMultimaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> HotRodSyncMultimap<K, V> get(String name) {
      return new HotRodSyncMultimap(hotrod, name);
   }

   @Override
   public <K, V> HotRodSyncMultimap<K, V> create(String name, MultimapConfiguration cacheConfiguration) {
      return new HotRodSyncMultimap<>(hotrod, name);
   }

   @Override
   public <K, V> HotRodSyncMultimap<K, V> create(String name, String template) {
      return new HotRodSyncMultimap<>(hotrod, name);
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
