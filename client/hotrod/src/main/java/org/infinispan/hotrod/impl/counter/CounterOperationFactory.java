package org.infinispan.hotrod.impl.counter;

import java.net.SocketAddress;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.counter.operation.AddListenerOperation;
import org.infinispan.hotrod.impl.counter.operation.AddOperation;
import org.infinispan.hotrod.impl.counter.operation.CompareAndSwapOperation;
import org.infinispan.hotrod.impl.counter.operation.DefineCounterOperation;
import org.infinispan.hotrod.impl.counter.operation.GetConfigurationOperation;
import org.infinispan.hotrod.impl.counter.operation.GetCounterNamesOperation;
import org.infinispan.hotrod.impl.counter.operation.GetValueOperation;
import org.infinispan.hotrod.impl.counter.operation.IsDefinedOperation;
import org.infinispan.hotrod.impl.counter.operation.RemoveListenerOperation;
import org.infinispan.hotrod.impl.counter.operation.RemoveOperation;
import org.infinispan.hotrod.impl.counter.operation.ResetOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

/**
 * A operation factory that builds counter operations.
 *
 * @since 14.0
 */
public class CounterOperationFactory {

   public static final byte[] COUNTER_CACHE_NAME = HotRodTransport.cacheNameBytes("org.infinispan.COUNTER");
   private final OperationContext operationContext;

   CounterOperationFactory(HotRodConfiguration configuration, ChannelFactory channelFactory, Codec codec) {
      this.operationContext = new OperationContext(channelFactory, codec, null, configuration, null, "org.infinispan.COUNTER");
   }

   IsDefinedOperation newIsDefinedOperation(String counterName) {
      return new IsDefinedOperation(operationContext, counterName);
   }

   GetConfigurationOperation newGetConfigurationOperation(String counterName) {
      return new GetConfigurationOperation(operationContext, counterName);
   }

   DefineCounterOperation newDefineCounterOperation(String counterName, CounterConfiguration cfg) {
      return new DefineCounterOperation(operationContext, counterName, cfg);
   }

   RemoveOperation newRemoveOperation(String counterName, boolean useConsistentHash) {
      return new RemoveOperation(operationContext, counterName, useConsistentHash);
   }

   AddOperation newAddOperation(String counterName, long delta, boolean useConsistentHash) {
      return new AddOperation(operationContext, counterName, delta, useConsistentHash);
   }

   GetValueOperation newGetValueOperation(String counterName, boolean useConsistentHash) {
      return new GetValueOperation(operationContext, counterName, useConsistentHash);
   }

   ResetOperation newResetOperation(String counterName, boolean useConsistentHash) {
      return new ResetOperation(operationContext, counterName, useConsistentHash);
   }

   CompareAndSwapOperation newCompareAndSwapOperation(String counterName, long expect, long update,
                                                      CounterConfiguration counterConfiguration) {
      return new CompareAndSwapOperation(operationContext, counterName, expect, update, counterConfiguration);
   }

   GetCounterNamesOperation newGetCounterNamesOperation() {
      return new GetCounterNamesOperation(operationContext);
   }

   AddListenerOperation newAddListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new AddListenerOperation(operationContext, counterName, listenerId, server);
   }

   RemoveListenerOperation newRemoveListenerOperation(String counterName, byte[] listenerId, SocketAddress server) {
      return new RemoveListenerOperation(operationContext, counterName, listenerId, server);
   }
}
