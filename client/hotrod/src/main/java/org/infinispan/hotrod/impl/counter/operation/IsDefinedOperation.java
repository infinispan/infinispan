package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link CounterManager#isDefined(String)}.
 *
 * @since 14.0
 */
public class IsDefinedOperation extends BaseCounterOperation<Boolean> {

   public IsDefinedOperation(OperationContext operationContext, String counterName) {
      super(operationContext, COUNTER_IS_DEFINED_REQUEST, COUNTER_IS_DEFINED_RESPONSE, counterName, false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(status == NO_ERROR_STATUS);
   }
}
