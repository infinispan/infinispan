package org.infinispan.counter.exception;

import static java.lang.String.format;

import org.infinispan.counter.api.StrongCounter;

/**
 * A {@link CounterException} signalling that the {@link StrongCounter} has reached its bounds.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterOutOfBoundsException extends CounterException {

   public static final String FORMAT_MESSAGE = "%s reached.";
   public static final String UPPER_BOUND = "Upper bound";
   public static final String LOWER_BOUND = "Lower bound";

   public CounterOutOfBoundsException(String message) {
      super(message);
   }

   public boolean isUpperBoundReached() {
      return getMessage().endsWith(format(FORMAT_MESSAGE, UPPER_BOUND));
   }

   public boolean isLowerBoundReached() {
      return getMessage().endsWith(format(FORMAT_MESSAGE, LOWER_BOUND));
   }
}
