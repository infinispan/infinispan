package org.infinispan.counter.impl.factory;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.impl.manager.CounterConfigurationManager;
import org.infinispan.counter.impl.manager.CounterConfigurationStorage;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.manager.PersistedCounterConfigurationStorage;
import org.infinispan.counter.impl.manager.VolatileCounterConfigurationStorage;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * {@link ComponentFactory} for counters.
 *
 * @since 14.0
 */
@DefaultFactoryFor(classes = {
      WeakCounterFactory.class,
      StrongCounterFactory.class,
      CounterManagerNotificationManager.class,
      CounterConfigurationManager.class,
      CounterConfigurationStorage.class,
      CounterManager.class,
      EmbeddedCounterManager.class
})
@Scope(Scopes.GLOBAL)
public class CounterComponentsFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String name) {
      if (name.equals(WeakCounterFactory.class.getName())) {
         return new CacheBasedWeakCounterFactory();
      } else if (name.equals(StrongCounterFactory.class.getName())) {
         return new CacheBasedStrongCounterFactory();
      } else if (name.equals(CounterManagerNotificationManager.class.getName())) {
         return new CounterManagerNotificationManager();
      } else if (name.equals(CounterConfigurationManager.class.getName())) {
         return new CounterConfigurationManager(globalConfiguration);
      } else if (name.equals(CounterManager.class.getName())) {
         return new EmbeddedCounterManager();
      } else if (name.equals(EmbeddedCounterManager.class.getName())) {
         return ComponentAlias.of(CounterManager.class);
      } else if (name.equals(CounterConfigurationStorage.class.getName())) {
         return globalConfiguration.globalState().enabled() ?
               new PersistedCounterConfigurationStorage(globalConfiguration) :
               VolatileCounterConfigurationStorage.INSTANCE;
      }
      throw CONTAINER.factoryCannotConstructComponent(name);
   }
}
