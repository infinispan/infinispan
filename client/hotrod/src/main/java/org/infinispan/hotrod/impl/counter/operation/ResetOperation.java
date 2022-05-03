package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link StrongCounter#reset()} and {@link WeakCounter#reset()}.
 *
 * @since 14.0
 */
public class ResetOperation extends BaseCounterOperation<Void> {

   public ResetOperation(OperationContext operationContext, String counterName, boolean useConsistentHash) {
      super(operationContext, COUNTER_RESET_REQUEST, COUNTER_RESET_RESPONSE, counterName, useConsistentHash);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      complete(null);
   }
}
