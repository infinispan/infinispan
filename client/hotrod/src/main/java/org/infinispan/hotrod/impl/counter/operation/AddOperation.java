package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Add operation.
 * <p>
 * Adds the {@code delta} to the counter's value and returns the result.
 * <p>
 * It can throw a {@link CounterOutOfBoundsException} if the counter is bounded and the it has been reached.
 *
 * @since 14.0
 */
public class AddOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(AddOperation.class, Log.class);

   private final long delta;

   public AddOperation(OperationContext operationContext,
                       String counterName, long delta, boolean useConsistentHash) {
      super(operationContext, COUNTER_ADD_AND_GET_REQUEST, COUNTER_ADD_AND_GET_RESPONSE, counterName, useConsistentHash);
      this.delta = delta;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 8);
      buf.writeLong(delta);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      complete(buf.readLong());
   }

   private void assertBoundaries(short status) {
      if (status == NOT_EXECUTED_WITH_PREVIOUS) {
         if (delta > 0) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
