package org.infinispan.counter;

import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.SyncWeakCounterAdapter;

/**
 * A {@link WeakCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see WeakCounter
 * @since 9.0
 * @deprecated since 9.2. Use {@link WeakCounter#sync()} instead. It will be removed in 10.0
 */
@Deprecated
public class SyncWeakCounter extends SyncWeakCounterAdapter {

   public SyncWeakCounter(WeakCounter counter) {
      super(counter);
   }
}
