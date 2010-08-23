package org.infinispan.client.hotrod.exceptions;

/**
 * Signals an internal protocol error.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class InvalidResponseException extends HotRodClientException {
   public InvalidResponseException() {
   }

   public InvalidResponseException(String message) {
      super(message);
   }

   public InvalidResponseException(String message, Throwable cause) {
      super(message, cause);
   }

   public InvalidResponseException(Throwable cause) {
      super(cause);
   }
}
