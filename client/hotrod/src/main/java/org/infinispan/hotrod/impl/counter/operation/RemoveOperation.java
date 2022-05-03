package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link CounterManager#remove(String)}, {@link StrongCounter#remove()} and {@link
 * WeakCounter#remove()}.
 *
 * @since 14.0
 */
public class RemoveOperation extends BaseCounterOperation<Void> {
   public RemoveOperation(OperationContext operationContext, String counterName, boolean useConsistentHash) {
      super(operationContext, COUNTER_REMOVE_REQUEST, COUNTER_REMOVE_RESPONSE, counterName, useConsistentHash);
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
