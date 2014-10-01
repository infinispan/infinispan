package org.infinispan.client.hotrod.exceptions;

/**
 * This exception is thrown when the remote cache or cache manager does not
 * have the right lifecycle state for operations to be called on it. Situations
 * like this include when the cache is stopping or is stopped, when the cache
 * manager is stopped...etc.
 *
 * @since 7.0
 */
public class RemoteIllegalLifecycleStateException extends HotRodClientException {

   public RemoteIllegalLifecycleStateException(String msgFromServer, long messageId, short status) {
      super(msgFromServer, messageId, status);
   }

}
