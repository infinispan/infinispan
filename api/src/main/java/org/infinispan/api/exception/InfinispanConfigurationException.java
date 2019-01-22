package org.infinispan.api.exception;

/**
 * Exception raised when a configuration error is found
 *
 * @since 10.0
 * @author Katia Aresti, karesti@redhat.com
 */
public class InfinispanConfigurationException extends InfinispanException {
   public InfinispanConfigurationException(String message) {
      super(message);
   }
}
