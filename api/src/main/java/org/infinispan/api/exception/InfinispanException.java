package org.infinispan.api.exception;

/**
 *
 * @since 10.0
 */
public class InfinispanException extends RuntimeException {
   public InfinispanException(String message) {
      super(message);
   }

   public InfinispanException(String message, Throwable throwable) {
      super(message, throwable);
   }
}
