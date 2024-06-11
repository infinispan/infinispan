package org.infinispan.client.hotrod.exceptions;

/**
 * Signals an remote timeout(due to locking) in the infinispan server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodTimeoutException extends HotRodClientException {
   public HotRodTimeoutException() {
   }

   public HotRodTimeoutException(String message) {
      super(message);
   }

   public HotRodTimeoutException(Throwable cause) {
      super(cause);
   }

   public HotRodTimeoutException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodTimeoutException(String remoteMessage, long messageId, int errorStatusCode) {
      super(remoteMessage, messageId, errorStatusCode);
   }
}
