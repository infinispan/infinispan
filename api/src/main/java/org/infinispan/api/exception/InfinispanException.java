package org.infinispan.api.exception;

/**
 * InfinispanException is raised when any runtime error not related to configuration is raised. It wraps the underlying
 * exception/error.
 *
 * @since 14.0
 */
public class InfinispanException extends RuntimeException {
   public InfinispanException(String message) {
      super(message);
   }

   public InfinispanException(String message, Throwable throwable) {
      super(message, throwable);
   }
}
