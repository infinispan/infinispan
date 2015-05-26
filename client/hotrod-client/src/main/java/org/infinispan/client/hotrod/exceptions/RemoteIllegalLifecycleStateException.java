package org.infinispan.client.hotrod.exceptions;

import java.net.SocketAddress;

/**
 * This exception is thrown when the remote cache or cache manager does not
 * have the right lifecycle state for operations to be called on it. Situations
 * like this include when the cache is stopping or is stopped, when the cache
 * manager is stopped...etc.
 *
 * @since 7.0
 */
public class RemoteIllegalLifecycleStateException extends HotRodClientException {

   private final SocketAddress serverAddress;

   @Deprecated
   public RemoteIllegalLifecycleStateException(String msgFromServer, long messageId, short status) {
      super(msgFromServer, messageId, status);
      this.serverAddress = null;
   }

   public RemoteIllegalLifecycleStateException(String msgFromServer, long messageId, short status, SocketAddress serverAddress) {
      super(msgFromServer, messageId, status);
      this.serverAddress = serverAddress;
   }

   public SocketAddress getServerAddress() {
      return serverAddress;
   }

}
