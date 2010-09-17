package org.infinispan.client.hotrod.exceptions;

/**
 * Base class for exceptions reported by the hot rod client.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodClientException extends RuntimeException {
   private long messageId;
   private int errorStatusCode;

   public HotRodClientException() {
   }

   public HotRodClientException(String message) {
      super(message);
   }

   public HotRodClientException(Throwable cause) {
      super(cause);
   }

   public HotRodClientException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodClientException(String remoteMessage, long messageId, int errorStatusCode) {
      super(remoteMessage);
      this.messageId = messageId;
      this.errorStatusCode = errorStatusCode;
   }


   @Override
   public String toString() {
      return "HotRodServerException{" +
            "messageId=" + messageId +
            ", errorStatusCode=" + errorStatusCode +
            "} " + super.toString();
   }
}
