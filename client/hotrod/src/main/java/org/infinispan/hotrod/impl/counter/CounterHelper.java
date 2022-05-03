package org.infinispan.hotrod.impl.counter;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A helper class for {@link StrongCounter} and {@link WeakCounter}.
 *
 * @since 14.0
 */
class CounterHelper {

   private final CounterOperationFactory factory;

   CounterHelper(CounterOperationFactory factory) {
      this.factory = factory;
   }


}
