package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;

/**
 * A counter operation for {@link CounterManager#isDefined(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class IsDefinedOperation extends BaseCounterOperation<Boolean> {

   public IsDefinedOperation(String counterName) {
      super(counterName, false);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return status == NO_ERROR_STATUS;
   }

   @Override
   public short requestOpCode() {
      return COUNTER_IS_DEFINED_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_IS_DEFINED_RESPONSE;
   }
}
