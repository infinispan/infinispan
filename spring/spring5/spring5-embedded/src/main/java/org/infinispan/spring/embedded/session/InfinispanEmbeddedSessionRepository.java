package org.infinispan.spring.embedded.session;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;

/**
 * Session Repository for Infinispan in Embedded mode.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class InfinispanEmbeddedSessionRepository extends AbstractInfinispanSessionRepository {

   /**
    * Creates new repository based on {@link SpringCache}
    *
    * @param cache Cache which shall be used for session repository.
    */
   public InfinispanEmbeddedSessionRepository(SpringCache cache) {
      super(cache, new EmbeddedApplicationPublishedBridge(cache));
   }

   @Override
   protected void removeFromCacheWithoutNotifications(String originalId) {
      Cache nativeCache = (Cache) cache.getNativeCache();
      nativeCache.getAdvancedCache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(originalId);
   }
}
