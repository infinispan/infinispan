package org.infinispan.counter.exception;

/**
 * Signals a missing configuration or an invalid configuration.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterConfigurationException extends CounterException {

   public CounterConfigurationException() {
   }

   public CounterConfigurationException(String message) {
      super(message);
   }

   public CounterConfigurationException(String message, Throwable cause) {
      super(message, cause);
   }

   public CounterConfigurationException(Throwable cause) {
      super(cause);
   }

}
