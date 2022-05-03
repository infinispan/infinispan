package org.infinispan.hotrod.exceptions;

import org.infinispan.hotrod.impl.cache.RemoteCache;

/**
 * Thrown when trying to use an {@link RemoteCache} that is associated to an
 * {@link org.infinispan.hotrod.RemoteCacheManager} that was not started.
 *
 * @since 14.0
 */
public class RemoteCacheManagerNotStartedException extends HotRodClientException {

   public RemoteCacheManagerNotStartedException(String message) {
      super(message);
   }
}
