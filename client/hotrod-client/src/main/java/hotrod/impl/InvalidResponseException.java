package hotrod.impl;

import hotrod.HotRodException;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class InvalidResponseException extends HotRodException {
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
