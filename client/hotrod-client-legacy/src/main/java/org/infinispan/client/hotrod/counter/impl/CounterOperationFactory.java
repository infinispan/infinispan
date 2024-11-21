package org.infinispan.client.hotrod.counter.impl;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

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
import org.infinispan.client.hotrod.counter.operation.SetOperation;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.counter.api.CounterConfiguration;

/**
 * A operation factory that builds counter operations.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterOperationFactory {

   public static final byte[] COUNTER_CACHE_NAME = RemoteCacheManager.cacheNameBytes("org.infinispan.COUNTER");

   private final Configuration configuration;
   private final ChannelFactory channelFactory;
   private final AtomicReference<ClientTopology> clientTopologyRef;

   CounterOperationFactory(Configuration configuration, ChannelFactory channelFactory) {
      this.configuration = configuration;
      this.channelFactory = channelFactory;
      clientTopologyRef = channelFactory.createTopologyId(COUNTER_CACHE_NAME);
   }

   IsDefinedOperation newIsDefinedOperation(String counterName) {
      return new IsDefinedOperation(channelFactory, clientTopologyRef, configuration, counterName);
   }

   GetConfigurationOperation newGetConfigurationOperation(String counterName) {
      return new GetConfigurationOperation(channelFactory, clientTopologyRef, configuration, counterName);
   }

   DefineCounterOperation newDefineCounterOperation(String counterName, CounterConfiguration cfg) {
      return new DefineCounterOperation(channelFactory, clientTopologyRef, configuration, counterName, cfg);
   }

   RemoveOperation newRemoveOperation(String counterName, boolean useConsistentHash) {
      return new RemoveOperation(channelFactory, clientTopologyRef, configuration, counterName, useConsistentHash);
   }

   AddOperation newAddOperation(String counterName, long delta, boolean useConsistentHash) {
      return new AddOperation(channelFactory, clientTopologyRef, configuration, counterName, delta, useConsistentHash);
   }

   GetValueOperation newGetValueOperation(String counterName, boolean useConsistentHash) {
      return new GetValueOperation(channelFactory, clientTopologyRef, configuration, counterName, useConsistentHash);
   }

   ResetOperation newResetOperation(String counterName, boolean useConsistentHash) {
      return new ResetOperation(channelFactory, clientTopologyRef, configuration, counterName, useConsistentHash);
   }

   CompareAndSwapOperation newCompareAndSwapOperation(String counterName, long expect, long update,
                                                      CounterConfiguration counterConfiguration) {
      return new CompareAndSwapOperation(channelFactory, clientTopologyRef, configuration, counterName, expect,
            update, counterConfiguration);
   }

   SetOperation newSetOperation(String counterName, long value, boolean useConsistentHash) {
      return new SetOperation(channelFactory, clientTopologyRef, configuration, counterName, value, useConsistentHash);
   }

   GetCounterNamesOperation newGetCounterNamesOperation() {
      return new GetCounterNamesOperation(channelFactory, clientTopologyRef, configuration);
   }

   AddListenerOperation newAddListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new AddListenerOperation(channelFactory, clientTopologyRef, configuration, counterName, listenerId,
            server);
   }

   RemoveListenerOperation newRemoveListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new RemoveListenerOperation(channelFactory, clientTopologyRef, configuration, counterName, listenerId,
            server);
   }
}
