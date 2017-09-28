package org.infinispan.counter.impl.strong;

import java.util.concurrent.CompletionException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.logging.Log;

/**
 * An unbounded strong consistent counter.
 *
 * @author Pedro Ruivo
 * @see AbstractStrongCounter
 * @since 9.0
 */
public class UnboundedStrongCounter extends AbstractStrongCounter {

   private static final Log log = LogFactory.getLog(AbstractStrongCounter.class, Log.class);

   public UnboundedStrongCounter(String counterName, AdvancedCache<StrongCounterKey, CounterValue> cache,
         CounterConfiguration configuration, CounterManagerNotificationManager notificationManager) {
      super(counterName, cache, configuration, notificationManager);
   }

   @Override
   protected Boolean handleCASResult(CounterState state) {
      return state == CounterState.VALID;
   }

   @Override
   protected long handleAddResult(CounterValue counterValue) {
      if (counterValue == null) {
         throw new CompletionException(log.counterDeleted());
      }
      return counterValue.getValue();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public String toString() {
      return "UnboundedStrongCounter{" +
            "counterName=" + key.getCounterName() +
            '}';
   }
}
