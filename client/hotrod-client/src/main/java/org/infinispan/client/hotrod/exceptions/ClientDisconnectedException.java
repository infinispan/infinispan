package org.infinispan.client.hotrod.exceptions;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class ClientDisconnectedException extends HotRodException {
   public ClientDisconnectedException() {
      super("Cannot call a method on the remote cache after RemoteCacheFactory.stop() has been called.");
   }
}
