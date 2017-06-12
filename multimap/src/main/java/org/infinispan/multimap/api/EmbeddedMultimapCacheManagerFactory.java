package org.infinispan.multimap.api;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;

/**
 * A {@link MultimapCache} factory for embedded cached.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class EmbeddedMultimapCacheManagerFactory {

   private EmbeddedMultimapCacheManagerFactory() {
   }

   public static MultimapCacheManager from(EmbeddedCacheManager cacheManager) {
      return new EmbeddedMultimapCacheManager(cacheManager);
   }
}
