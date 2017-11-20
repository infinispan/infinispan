package org.infinispan.client.hotrod.counter.impl;

import org.infinispan.counter.api.CounterConfiguration;
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

   long addAndGet(String name, long delta) {
      return factory.newAddOperation(name, delta).execute();
   }

   long compareAndSwap(String name, long expect, long update, CounterConfiguration configuration) {
      return factory.newCompareAndSwapOperation(name, expect, update, configuration).execute();
   }

   long getValue(String name) {
      return factory.newGetValueOperation(name).execute();
   }

   void reset(String name) {
      factory.newResetOperation(name).execute();
   }

   void remove(String name) {
      factory.newRemoveOperation(name).execute();
   }
}
