package org.infinispan.client.hotrod.counter.impl;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.logging.Log;
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

   private static final Log commonsLog = LogFactory.getLog(RemoteCounterManager.class, Log.class);
   private final Map<String, Object> counters;
   private CounterOperationFactory factory;
   private NotificationManager notificationManager;

   public RemoteCounterManager() {
      counters = new ConcurrentHashMap<>();
   }

   public void start(ChannelFactory channelFactory, Codec codec, Configuration configuration, ClientListenerNotifier listenerNotifier) {
      this.factory = new CounterOperationFactory(configuration, channelFactory, codec);
      this.notificationManager = new NotificationManager(listenerNotifier, factory);
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      return getOrCreateCounter(name, StrongCounter.class, this::createStrongCounter,
            () -> commonsLog.invalidCounterType("Strong", "Weak"));
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      return getOrCreateCounter(name, WeakCounter.class, this::createWeakCounter,
            () -> commonsLog.invalidCounterType("Weak", "Strong"));
   }

   @Override
   public boolean defineCounter(String name, CounterConfiguration configuration) {
      return await(factory.newDefineCounterOperation(name, configuration).execute());
   }

   @Override
   public boolean isDefined(String name) {
      return await(factory.newIsDefinedOperation(name).execute());
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      return await(factory.newGetConfigurationOperation(counterName).execute());
   }

   @Override
   public void remove(String counterName) {
      await(factory.newRemoveOperation(counterName).execute());
   }

   @Override
   public Collection<String> getCounterNames() {
      return await(factory.newGetCounterNamesOperation().execute());
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
         throw commonsLog.invalidCounterType("Weak", "Strong");
      }
   }

   private WeakCounter createWeakCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw commonsLog.undefinedCounter(counterName);
      }
      assertWeakCounter(configuration);
      return new WeakCounterImpl(counterName, configuration, factory, notificationManager);
   }

   private StrongCounter createStrongCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw commonsLog.undefinedCounter(counterName);
      }
      assertStrongCounter(configuration);
      return new StrongCounterImpl(counterName, configuration, factory, notificationManager);
   }

   private void assertStrongCounter(CounterConfiguration configuration) {
      if (configuration.type() == CounterType.WEAK) {
         throw commonsLog.invalidCounterType("Strong", "Weak");
      }
   }
}
