package org.infinispan.hotrod.exceptions;

/**
 * Signals an internal protocol error.
 *
 * @since 14.0
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
