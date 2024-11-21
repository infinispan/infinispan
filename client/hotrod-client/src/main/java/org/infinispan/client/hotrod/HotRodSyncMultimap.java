package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncMultimap;

/**
 * @since 14.0
 **/
@Experimental
final class HotRodSyncMultimap<K, V> implements SyncMultimap<K, V> {
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
      throw new UnsupportedOperationException();
   }

   @Override
   public SyncContainer container() {
      return hotrod.sync();
   }

   @Override
   public void add(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CloseableIterable<V> get(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsKey(K key) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsEntry(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public long estimateSize() {
      throw new UnsupportedOperationException();
   }
}
