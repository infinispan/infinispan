package org.infinispan.hotrod;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultiMapConfiguration;
import org.infinispan.api.sync.SyncMultiMaps;

/**
 * @since 14.0
 **/
public class HotRodSyncMultiMaps implements SyncMultiMaps {
   private final HotRod hotrod;

   public HotRodSyncMultiMaps(HotRod hotrod) {
      this.hotrod = hotrod;
   }

   @Override
   public <K, V> HotRodSyncMultiMap<K, V> get(String name) {
      return new HotRodSyncMultiMap(hotrod, name);
   }

   @Override
   public <K, V> HotRodSyncMultiMap<K, V> create(String name, MultiMapConfiguration cacheConfiguration) {
      return new HotRodSyncMultiMap<>(hotrod, name);
   }

   @Override
   public <K, V> HotRodSyncMultiMap<K, V> create(String name, String template) {
      return new HotRodSyncMultiMap<>(hotrod, name);
   }

   @Override
   public void remove(String name) {

   }

   @Override
   public CloseableIterable<String> names() {
      return null;
   }

   @Override
   public void createTemplate(String name, MultiMapConfiguration cacheConfiguration) {

   }

   @Override
   public void removeTemplate(String name) {

   }

   @Override
   public CloseableIterable<String> templateNames() {
      return null;
   }
}
