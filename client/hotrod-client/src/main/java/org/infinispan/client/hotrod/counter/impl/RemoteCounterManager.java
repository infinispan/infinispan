package org.infinispan.client.hotrod.counter.impl;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
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
public class RemoteCounterManager implements CounterManager, Consumer<Set<SocketAddress>> {

   private static final Log commonsLog = LogFactory.getLog(RemoteCounterManager.class, Log.class);
   private final Map<String, Object> counters;
   private CounterOperationFactory factory;
   private ExecutorService executorService;
   private CounterHelper counterHelper;
   private NotificationManager notificationManager;

   public RemoteCounterManager() {
      counters = new ConcurrentHashMap<>();
   }

   public void start(TransportFactory transportFactory, Codec codec, Configuration configuration,
         ExecutorService executorService) {
      this.factory = new CounterOperationFactory(configuration, transportFactory, codec);
      this.executorService = executorService;
      this.counterHelper = new CounterHelper(factory);
      this.notificationManager = new NotificationManager(factory);
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
      return factory.newDefineCounterOperation(name, configuration).execute();
   }

   @Override
   public boolean isDefined(String name) {
      return factory.newIsDefinedOperation(name).execute();
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      return factory.newGetConfigurationOperation(counterName).execute();
   }

   @Override
   public void remove(String counterName) {
      factory.newRemoveOperation(counterName).execute();
   }

   @Override
   public Collection<String> getCounterNames() {
      return factory.newGetCounterNamesOperation().execute();
   }

   /**
    * Failed servers callback!
    */
   @Override
   public void accept(Set<SocketAddress> socketAddresses) {
      //ping happens during start before notification manager is created!
      if (notificationManager != null) {
         notificationManager.failedServer(socketAddresses);
      }
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
      return new WeakCounterImpl(counterName, configuration, executorService, counterHelper, notificationManager);
   }

   private StrongCounter createStrongCounter(String counterName) {
      CounterConfiguration configuration = getConfiguration(counterName);
      if (configuration == null) {
         throw commonsLog.undefinedCounter(counterName);
      }
      assertStrongCounter(configuration);
      return new StrongCounterImpl(counterName, configuration, executorService, counterHelper, notificationManager);
   }

   private void assertStrongCounter(CounterConfiguration configuration) {
      if (configuration.type() == CounterType.WEAK) {
         throw commonsLog.invalidCounterType("Strong", "Weak");
      }
   }
}
