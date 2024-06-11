package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

import io.netty.buffer.ByteBuf;

/**
 * A counter operation for {@link StrongCounter#reset()} and {@link WeakCounter#reset()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class ResetOperation extends BaseCounterOperation<Void> {

   public ResetOperation(String counterName, boolean useConsistentHash) {
      super(counterName, useConsistentHash);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      return null;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_RESET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_RESET_RESPONSE;
   }
}
