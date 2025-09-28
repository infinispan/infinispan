package org.infinispan.client.hotrod.counter.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterException;

/**
 * A {@link CounterManager} implementation for Hot Rod clients.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoteCounterManager implements CounterManager {
   private final Map<String, Object> counters;
   private CounterOperationFactory factory;
   private OperationDispatcher dispatcher;
   private NotificationManager notificationManager;

   public RemoteCounterManager() {
      counters = new ConcurrentHashMap<>();
   }

   public void start(OperationDispatcher dispatcher, ClientListenerNotifier listenerNotifier) {
      this.factory = new CounterOperationFactory();
      this.dispatcher = dispatcher;
      this.notificationManager = new NotificationManager(listenerNotifier, factory, dispatcher);

      dispatcher.addCacheTopologyInfoIfAbsent(CounterOperationFactory.COUNTER_CACHE_NAME);
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      return getOrCreateCounter(name, StrongCounter.class, this::createStrongCounter,
            () -> Log.HOTROD.invalidCounterType("Strong", "Weak"));
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      return getOrCreateCounter(name, WeakCounter.class, this::createWeakCounter,
            () -> Log.HOTROD.invalidCounterType("Weak", "Strong"));
   }

   @Override
   public boolean defineCounter(String name, CounterConfiguration configuration) {
      return dispatcher.await(dispatcher.execute(factory.newDefineCounterOperation(name, configuration)));
   }

   @Override
   public void undefineCounter(String name) {
   }

   @Override
   public boolean isDefined(String name) {
      return dispatcher.await(dispatcher.execute(factory.newIsDefinedOperation(name)));
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      return dispatcher.await(dispatcher.execute(factory.newGetConfigurationOperation(counterName)));
   }

   @Override
   public void remove(String counterName) {
      dispatcher.await(dispatcher.execute(factory.newRemoveOperation(counterName, true)));
   }

   @Override
   public Collection<String> getCounterNames() {
      return dispatcher.await(dispatcher.execute(factory.newGetCounterNamesOperation()));
   }

   public void stop() {
      if (notificationManager != null) {
         notificationManager.stop();
      }
   }

   private <T> T getOrCreateCounter(String name, Class<T> tClass, Function<String, T> createFunction,
         Supplier<CounterException> invalidCounter) {
      Object counter = counters.computeIfAbsent(name, createFunction);
      if (!tClass.isInstance(counter)) {
         throw invalidCounter.get();
      }
      return tClass.cast(counter);
   }

   private void assertWeakCounter(CounterConfiguration configuration) {
      if (configuration.type() != CounterType.WEAK) {
         throw Log.HOTROD.invalidCounterType("Weak", "Strong");
      }
   }

   private WeakCounter createWeakCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw Log.HOTROD.undefinedCounter(counterName);
      }
      assertWeakCounter(configuration);
      return new WeakCounterImpl(counterName, configuration, factory, dispatcher, notificationManager);
   }

   private StrongCounter createStrongCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw Log.HOTROD.undefinedCounter(counterName);
      }
      assertStrongCounter(configuration);
      return new StrongCounterImpl(counterName, configuration, factory, dispatcher, notificationManager);
   }

   private void assertStrongCounter(CounterConfiguration configuration) {
      if (configuration.type() == CounterType.WEAK) {
         throw Log.HOTROD.invalidCounterType("Strong", "Weak");
      }
   }
}
