package org.infinispan.client.hotrod.counter.impl;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A helper class for {@link StrongCounter} and {@link WeakCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class CounterHelper {

   private final CounterOperationFactory factory;

   CounterHelper(CounterOperationFactory factory) {
      this.factory = factory;
   }


}
