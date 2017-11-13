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
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.counter.api.CounterConfiguration;

/**
 * A operation factory that builds counter operations.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
class CounterOperationFactory {

   private final Configuration configuration;
   private final ChannelFactory channelFactory;
   private final Codec codec;
   private final AtomicInteger topologyId;

   CounterOperationFactory(Configuration configuration, ChannelFactory channelFactory, Codec codec) {
      this.configuration = configuration;
      this.channelFactory = channelFactory;
      this.codec = codec;
      this.topologyId = channelFactory.createTopologyId(RemoteCacheManager.cacheNameBytes("org.infinispan.counter"));
   }

   ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   Codec getCodec() {
      return codec;
   }

   IsDefinedOperation newIsDefinedOperation(String counterName) {
      return new IsDefinedOperation(codec, channelFactory, topologyId, configuration, counterName);
   }

   GetConfigurationOperation newGetConfigurationOperation(String counterName) {
      return new GetConfigurationOperation(codec, channelFactory, topologyId, configuration, counterName);
   }

   DefineCounterOperation newDefineCounterOperation(String counterName, CounterConfiguration cfg) {
      return new DefineCounterOperation(codec, channelFactory, topologyId, configuration, counterName, cfg);
   }

   RemoveOperation newRemoveOperation(String counterName) {
      return new RemoveOperation(codec, channelFactory, topologyId, configuration, counterName);
   }

   AddOperation newAddOperation(String counterName, long delta) {
      return new AddOperation(codec, channelFactory, topologyId, configuration, counterName, delta);
   }

   GetValueOperation newGetValueOperation(String counterName) {
      return new GetValueOperation(codec, channelFactory, topologyId, configuration, counterName);
   }

   ResetOperation newResetOperation(String counterName) {
      return new ResetOperation(codec, channelFactory, topologyId, configuration, counterName);
   }

   CompareAndSwapOperation newCompareAndSwapOperation(String counterName, long expect, long update,
         CounterConfiguration counterConfiguration) {
      return new CompareAndSwapOperation(codec, channelFactory, topologyId, configuration, counterName, expect,
            update, counterConfiguration);
   }

   GetCounterNamesOperation newGetCounterNamesOperation() {
      return new GetCounterNamesOperation(codec, channelFactory, topologyId, configuration);
   }

   AddListenerOperation newAddListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new AddListenerOperation(codec, channelFactory, topologyId, configuration, counterName, listenerId,
            server);
   }

   RemoveListenerOperation newRemoveListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new RemoveListenerOperation(codec, channelFactory, topologyId, configuration, counterName, listenerId,
            server);
   }
}
