package org.infinispan.embedded;

import static org.infinispan.commons.util.concurrent.CompletableFutures.uncheckedAwait;
import static org.infinispan.embedded.impl.EmbeddedUtil.closeableIterable;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncMultimap;
import org.infinispan.multimap.api.embedded.MultimapCache;

/**
 * @since 15.0
 */
public class EmbeddedSyncMultimap<K, V> implements SyncMultimap<K, V> {
   private final Embedded embedded;
   private final MultimapCache<K, V> multimapCache;

   EmbeddedSyncMultimap(Embedded embedded, MultimapCache<K, V> multimapCache) {
      this.embedded = embedded;
      this.multimapCache = multimapCache;
   }

   @Override
   public String name() {
      return multimapCache.getName();
   }

   @Override
   public MultimapConfiguration configuration() {
      return null;
   }

   @Override
   public SyncContainer container() {
      return embedded.sync();
   }

   @Override
   public void add(K key, V value) {
      uncheckedAwait(multimapCache.put(key, value));
   }

   @Override
   public CloseableIterable<V> get(K key) {
      return closeableIterable(uncheckedAwait(multimapCache.get(key)));
   }

   @Override
   public boolean remove(K key) {
      return uncheckedAwait(multimapCache.remove(key));
   }

   @Override
   public boolean remove(K key, V value) {
      return uncheckedAwait(multimapCache.remove(key, value));
   }

   @Override
   public boolean containsKey(K key) {
      return uncheckedAwait(multimapCache.containsKey(key));
   }

   @Override
   public boolean containsEntry(K key, V value) {
      return uncheckedAwait(multimapCache.containsEntry(key, value));
   }

   @Override
   public long estimateSize() {
      return uncheckedAwait(multimapCache.size());
   }
}
