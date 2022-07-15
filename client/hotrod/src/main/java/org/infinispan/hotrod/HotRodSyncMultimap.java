package org.infinispan.hotrod;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncMultimap;

/**
 * @since 14.0
 **/
public class HotRodSyncMultimap<K, V> implements SyncMultimap<K, V> {
   private final HotRod hotrod;
   private final String name;

   HotRodSyncMultimap(HotRod hotrod, String name) {
      this.hotrod = hotrod;
      this.name = name;
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public MultimapConfiguration configuration() {
      return null;
   }

   @Override
   public HotRodSyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public void add(K key, V value) {

   }

   @Override
   public CloseableIterable<V> get(K key) {
      return null;
   }

   @Override
   public boolean remove(K key) {
      return false;
   }

   @Override
   public boolean remove(K key, V value) {
      return false;
   }

   @Override
   public boolean containsKey(K key) {
      return false;
   }

   @Override
   public boolean containsEntry(K key, V value) {
      return false;
   }

   @Override
   public long estimateSize() {
      return 0;
   }
}
