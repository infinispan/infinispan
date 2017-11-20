package org.infinispan.client.hotrod.counter.impl;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.counter.operation.AddListenerOperation;
import org.infinispan.client.hotrod.counter.operation.AddOperation;
import org.infinispan.client.hotrod.counter.operation.CompareAndSwapOperation;
import org.infinispan.client.hotrod.counter.operation.DefineCounterOperation;
import org.infinispan.client.hotrod.counter.operation.GetConfigurationOperation;
import org.infinispan.client.hotrod.counter.operation.GetCounterNamesOperation;
import org.infinispan.client.hotrod.counter.operation.GetValueOperation;
import org.infinispan.client.hotrod.counter.operation.IsDefinedOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveListenerOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveOperation;
import org.infinispan.client.hotrod.counter.operation.ResetOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterConfiguration;

/**
 * A operation factory that builds counter operations.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class CounterOperationFactory {

   private final Configuration configuration;
   private final TransportFactory transportFactory;
   private final Codec codec;
   private final AtomicInteger topologyId;

   CounterOperationFactory(Configuration configuration, TransportFactory transportFactory, Codec codec) {
      this.configuration = configuration;
      this.transportFactory = transportFactory;
      this.codec = codec;
      this.topologyId = transportFactory.createTopologyId(RemoteCacheManager.cacheNameBytes("org.infinispan.counter"));
   }

   TransportFactory geTransportFactory() {
      return transportFactory;
   }

   Codec getCodec() {
      return codec;
   }

   IsDefinedOperation newIsDefinedOperation(String counterName) {
      return new IsDefinedOperation(codec, transportFactory, topologyId, configuration, counterName);
   }

   GetConfigurationOperation newGetConfigurationOperation(String counterName) {
      return new GetConfigurationOperation(codec, transportFactory, topologyId, configuration, counterName);
   }

   DefineCounterOperation newDefineCounterOperation(String counterName, CounterConfiguration cfg) {
      return new DefineCounterOperation(codec, transportFactory, topologyId, configuration, counterName, cfg);
   }

   RemoveOperation newRemoveOperation(String counterName) {
      return new RemoveOperation(codec, transportFactory, topologyId, configuration, counterName);
   }

   AddOperation newAddOperation(String counterName, long delta) {
      return new AddOperation(codec, transportFactory, topologyId, configuration, counterName, delta);
   }

   GetValueOperation newGetValueOperation(String counterName) {
      return new GetValueOperation(codec, transportFactory, topologyId, configuration, counterName);
   }

   ResetOperation newResetOperation(String counterName) {
      return new ResetOperation(codec, transportFactory, topologyId, configuration, counterName);
   }

   CompareAndSwapOperation newCompareAndSwapOperation(String counterName, long expect, long update,
         CounterConfiguration counterConfiguration) {
      return new CompareAndSwapOperation(codec, transportFactory, topologyId, configuration, counterName, expect,
            update, counterConfiguration);
   }

   GetCounterNamesOperation newGetCounterNamesOperation() {
      return new GetCounterNamesOperation(codec, transportFactory, topologyId, configuration);
   }

   AddListenerOperation newAddListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new AddListenerOperation(codec, transportFactory, topologyId, configuration, counterName, listenerId,
            server);
   }

   RemoveListenerOperation newRemoveListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new RemoveListenerOperation(codec, transportFactory, topologyId, configuration, counterName, listenerId,
            server);
   }
}
