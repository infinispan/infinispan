package org.infinispan.counter.exception;

/**
 * Signal that an attempt to access a counter has failed.
 *
 * @since 14.0
 */
public class CounterNotFoundException extends CounterException {

   public CounterNotFoundException() {
   }

   public CounterNotFoundException(String message) {
      super(message);
   }

   public CounterNotFoundException(String message, Throwable cause) {
      super(message, cause);
   }

   public CounterNotFoundException(Throwable cause) {
      super(cause);
   }

   public CounterNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
