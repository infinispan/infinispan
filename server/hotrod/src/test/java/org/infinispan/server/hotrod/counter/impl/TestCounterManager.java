package org.infinispan.server.hotrod.counter.impl;

import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET_CONFIGURATION;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET_NAMES;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_IS_DEFINED;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_REMOVE;
import static org.infinispan.server.hotrod.OperationStatus.Success;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.hotrod.counter.op.CounterOp;
import org.infinispan.server.hotrod.counter.op.CreateCounterOp;
import org.infinispan.server.hotrod.counter.response.CounterConfigurationTestResponse;
import org.infinispan.server.hotrod.counter.response.CounterNamesTestResponse;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.Op;
import org.infinispan.server.hotrod.test.TestResponse;

/**
 * A {@link CounterManager} for Hot Rod server testing.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class TestCounterManager implements CounterManager {

   private final HotRodClient client;
   private final TestCounterNotificationManager notificationManager;
   private final Map<String, Object> counters;

   public TestCounterManager(HotRodClient client) {
      this.client = client;
      counters = new ConcurrentHashMap<>();
      notificationManager = new TestCounterNotificationManager(client);
      notificationManager.start();
   }

   @Override
   public StrongCounter getStrongCounter(String name) {
      StrongCounter c = (StrongCounter) counters.get(name);
      if (c == null) {
         CounterConfiguration config = getConfiguration(name);
         if (config == null || config.type() == CounterType.WEAK) {
            throw new IllegalStateException();
         }
         c = new TestStrongCounter(name, config, client, notificationManager);
         counters.put(name, c);
      }
      return c;
   }

   @Override
   public WeakCounter getWeakCounter(String name) {
      WeakCounter c = (WeakCounter) counters.get(name);
      if (c == null) {
         CounterConfiguration config = getConfiguration(name);
         if (config == null || config.type() != CounterType.WEAK) {
            throw new IllegalStateException();
         }
         c = new TestWeakCounter(name, config, client, notificationManager);
         counters.put(name, c);
      }
      return c;
   }

   @Override
   public boolean defineCounter(String name, CounterConfiguration configuration) {
      CreateCounterOp op = new CreateCounterOp(client.protocolVersion(), name, configuration);
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      return response.getStatus() == Success;
   }

   @Override
   public boolean isDefined(String name) {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_IS_DEFINED, name);
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      return response.getStatus() == Success;
   }

   @Override
   public CounterConfiguration getConfiguration(String counterName) {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_GET_CONFIGURATION, counterName);
      client.writeOp(op);
      TestResponse response = client.getResponse(op);
      return response.getStatus() == Success ?
             ((CounterConfigurationTestResponse) response).getConfiguration() :
             null;
   }

   @Override
   public void remove(String counterName) {
      CounterOp op = new CounterOp(client.protocolVersion(), COUNTER_REMOVE, counterName);
      client.writeOp(op);
      client.getResponse(op);
   }

   public Collection<String> getCounterNames() {
      Op op = new Op(0xA0, client.protocolVersion(), (byte) COUNTER_GET_NAMES.getRequestOpCode(), "", null, 0, 0, null,
            0, 0, (byte) 0, 0);
      client.writeOp(op);
      CounterNamesTestResponse response = (CounterNamesTestResponse) client.getResponse(op);
      return response.getCounterNames();
   }
}
