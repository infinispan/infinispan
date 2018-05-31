package org.infinispan.spring.session;

import org.infinispan.spring.provider.SpringCache;

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
}
