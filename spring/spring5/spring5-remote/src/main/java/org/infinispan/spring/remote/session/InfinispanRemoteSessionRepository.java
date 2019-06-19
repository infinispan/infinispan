package org.infinispan.spring.remote.session;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.CacheException;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;

/**
 * Session Repository for Infinispan in client/server mode.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class InfinispanRemoteSessionRepository extends AbstractInfinispanSessionRepository {
   /**
    * Creates new repository based on {@link SpringCache}
    *
    * @param cache Cache which shall be used for session repository.
    */
   public InfinispanRemoteSessionRepository(SpringCache cache) {
      super(cache, new RemoteApplicationPublishedBridge(cache));
   }

   @Override
   protected void removeFromCacheWithoutNotifications(String originalId) {
      RemoteCache remoteCache = (RemoteCache) cache.getNativeCache();
      if (cache.getWriteTimeout() > 0) {
         try {
            remoteCache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).removeAsync(originalId).get(cache.getWriteTimeout(), TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(e);
         } catch (ExecutionException | TimeoutException e) {
            throw new CacheException(e);
         }
      } else {
         remoteCache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(originalId);
      }
   }
}
