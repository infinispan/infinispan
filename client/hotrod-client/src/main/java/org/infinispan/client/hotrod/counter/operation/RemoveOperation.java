package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

import io.netty.buffer.ByteBuf;

/**
 * A counter operation for {@link CounterManager#remove(String)}, {@link StrongCounter#remove()} and {@link
 * WeakCounter#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveOperation extends BaseCounterOperation<Void> {
   public RemoveOperation(String counterName, boolean useConsistentHash) {
      super(counterName, useConsistentHash);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      return null;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_REMOVE_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_REMOVE_RESPONSE;
   }
}
