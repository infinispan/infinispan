package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * A counter operation that returns the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetValueOperation extends BaseCounterOperation<Long> {

   public GetValueOperation(String counterName, boolean useConsistentHash) {
      super(counterName, useConsistentHash);
   }

   @Override
   public Long createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      assert status == NO_ERROR_STATUS;
      return buf.readLong();
   }

   @Override
   public short requestOpCode() {
      return COUNTER_GET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_GET_RESPONSE;
   }
}
