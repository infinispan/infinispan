package org.infinispan.counter.impl.manager;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;

@DefaultFactoryFor(classes = CounterManager.class)
public class CounterManagerFactory implements ComponentFactory {
   @Inject EmbeddedCacheManager cacheManager;

   @Override
   public Object construct(String componentName) {
      return new EmbeddedCounterManager(cacheManager);
   }
}
