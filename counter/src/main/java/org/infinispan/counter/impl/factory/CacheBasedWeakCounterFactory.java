package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.counter.impl.weak.WeakCounterKey;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Creates {@link WeakCounter} stored in a {@link Cache}.
 *
 * @since 14.0
 */
@Scope(Scopes.GLOBAL)
public class CacheBasedWeakCounterFactory extends CacheBaseCounterFactory<WeakCounterKey> implements WeakCounterFactory {

   @Override
   public CompletionStage<WeakCounter> createWeakCounter(String name, CounterConfiguration configuration) {
      assert configuration.type() == CounterType.WEAK;
      return blockingManager.thenApplyBlocking(cache(configuration), cache -> {
         registerListeners(cache);
         WeakCounterImpl counter = new WeakCounterImpl(name, cache, configuration, notificationManager);
         counter.init();
         return counter;
      }, "create-weak-counter-" + name);
   }

   @Override
   public CompletionStage<Void> removeWeakCounter(String name, CounterConfiguration configuration) {
      return getCounterCacheAsync().thenCompose(cache -> WeakCounterImpl.removeWeakCounter(cache, configuration, name));
   }

   private void registerListeners(Cache<WeakCounterKey, CounterValue> cache) {
      // topology listener is used to compute the keys where this node is the primary owner
      // adds are made on these keys to avoid contention and improve performance
      notificationManager.registerTopologyListener(cache);
      // the weak counter keeps a local value and, on each event, the local value is updated (reads are always local)
      notificationManager.registerCounterValueListener(cache);
   }
}
