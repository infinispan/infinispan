package org.infinispan.api.exception;

/**
 * Exception raised when a configuration error is found
 *
 * @since 14.0
 */
public class InfinispanConfigurationException extends InfinispanException {
   public InfinispanConfigurationException(String message) {
      super(message);
   }
}
