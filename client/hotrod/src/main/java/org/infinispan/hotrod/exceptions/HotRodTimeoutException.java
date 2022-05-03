package org.infinispan.hotrod.exceptions;

/**
 * Signals an remote timeout(due to locking) in the infinispan server.
 *
 * @since 14.0
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
