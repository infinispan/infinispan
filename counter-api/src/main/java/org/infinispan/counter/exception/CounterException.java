package org.infinispan.counter.exception;

/**
 * A {@link RuntimeException} related to counters.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterException extends RuntimeException {

   public CounterException() {
   }

   public CounterException(String message) {
      super(message);
   }

   public CounterException(String message, Throwable cause) {
      super(message, cause);
   }

   public CounterException(Throwable cause) {
      super(cause);
   }

   public CounterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
