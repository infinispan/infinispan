package org.infinispan.counter;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.impl.SyncStrongCounterAdapter;

/**
 * A {@link StrongCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.0
 * @deprecated since 9.2. Use {@link StrongCounter#sync()} instead. It will be removed in 10.0
 */
@Deprecated
public class SyncStrongCounter extends SyncStrongCounterAdapter {

   public SyncStrongCounter(StrongCounter counter) {
      super(counter);
   }

}
