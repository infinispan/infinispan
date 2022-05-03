package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation that returns the counter's value.
 *
 * @since 14.0
 */
public class GetValueOperation extends BaseCounterOperation<Long> {

   public GetValueOperation(OperationContext operationContext, String counterName, boolean useConsistentHash) {
      super(operationContext, COUNTER_GET_REQUEST, COUNTER_GET_RESPONSE, counterName, useConsistentHash);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      assert status == NO_ERROR_STATUS;
      complete(buf.readLong());
   }
}
