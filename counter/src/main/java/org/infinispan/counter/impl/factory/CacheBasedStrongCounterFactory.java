package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.impl.strong.AbstractStrongCounter;
import org.infinispan.counter.impl.strong.BoundedStrongCounter;
import org.infinispan.counter.impl.strong.StrongCounterKey;
import org.infinispan.counter.impl.strong.UnboundedStrongCounter;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Created bounded and unbounded {@link StrongCounter} stored in a {@link Cache}.
 *
 * @since 14.0
 */
@Scope(Scopes.GLOBAL)
public class CacheBasedStrongCounterFactory extends CacheBaseCounterFactory<StrongCounterKey> implements StrongCounterFactory {

   @Override
   public CompletionStage<StrongCounter> createStrongCounter(String name, CounterConfiguration configuration) {
      assert configuration.type() != CounterType.WEAK;
      return blockingManager.thenApplyBlocking(cache(configuration), cache -> {
         AbstractStrongCounter counter =configuration.type() == CounterType.BOUNDED_STRONG ?
               new BoundedStrongCounter(name, cache, configuration, notificationManager) :
               new UnboundedStrongCounter(name, cache, configuration, notificationManager);
         counter.init();
         return counter;
      }, "create-strong-counter" + name);
   }

   @Override
   public CompletionStage<Void> removeStrongCounter(String name) {
      return getCounterCacheAsync().thenCompose(cache -> AbstractStrongCounter.removeStrongCounter(cache, name));
   }

}
