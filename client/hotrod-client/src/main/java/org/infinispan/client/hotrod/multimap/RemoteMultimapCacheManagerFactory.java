package org.infinispan.client.hotrod.multimap;

import org.infinispan.client.hotrod.RemoteCacheManager;

/**
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class RemoteMultimapCacheManagerFactory {

   private RemoteMultimapCacheManagerFactory() {
   }

   public static <K, V> MultimapCacheManager<K, V> from(RemoteCacheManager remoteCacheManager) {
      return new RemoteMultimapCacheManager<>(remoteCacheManager);
   }
}
