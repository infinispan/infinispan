package org.infinispan.counter.impl.strong;

import static org.infinispan.counter.exception.CounterOutOfBoundsException.LOWER_BOUND;
import static org.infinispan.counter.exception.CounterOutOfBoundsException.UPPER_BOUND;

import java.util.concurrent.CompletionException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.logging.Log;

/**
 * A bounded strong consistent counter.
 * <p>
 * When the boundaries are reached, a {@link CounterOutOfBoundsException} is thrown. Use {@link
 * CounterOutOfBoundsException#isUpperBoundReached()} or {@link CounterOutOfBoundsException#isLowerBoundReached()} to
 * check if upper or lower bound has been reached, respectively.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BoundedStrongCounter extends AbstractStrongCounter {

   private static final Log log = LogFactory.getLog(BoundedStrongCounter.class, Log.class);

   public BoundedStrongCounter(String counterName, AdvancedCache<StrongCounterKey, CounterValue> cache,
         CounterConfiguration configuration, CounterManagerNotificationManager notificationManager) {
      super(counterName, cache, configuration, notificationManager);
   }

   @Override
   protected long handleAddResult(CounterValue counterValue) {
      throwOutOfBoundExceptionIfNeeded(counterValue.getState());
      return counterValue.getValue();
   }

   protected Boolean handleCASResult(Object state) {
      if (state instanceof CounterState) {
         throwOutOfBoundExceptionIfNeeded((CounterState) state);
      }
      //noinspection ConstantConditions
      return (Boolean) state;
   }

   private void throwOutOfBoundExceptionIfNeeded(CounterState state) {
      switch (state) {
         case LOWER_BOUND_REACHED:
            throw new CompletionException(log.counterOurOfBounds(LOWER_BOUND));
         case UPPER_BOUND_REACHED:
            throw new CompletionException(log.counterOurOfBounds(UPPER_BOUND));
         default:
      }
   }

   @Override
   public String toString() {
      return "BoundedStrongCounter{" +
            "counterName=" + key.getCounterName() +
            '}';
   }
}
