package org.infinispan.client.hotrod.exceptions;

/**
 * Thrown when trying to use an {@link org.infinispan.client.hotrod.RemoteCache} that is associated to an
 * {@link org.infinispan.client.hotrod.RemoteCacheManager} that was not started.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheManagerNotStartedException extends HotRodClientException {

   public RemoteCacheManagerNotStartedException(String message) {
      super(message);
   }
}
