package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Set operation.
 * <p>
 * Sets the {@code value} to the counter's value and returns the old value.
 * <p>
 * It can throw a {@link CounterOutOfBoundsException} if the counter is bounded and has been reached.
 *
 * @author Dipanshu Gupta
 * @since 15.0
 */
public class SetOperation extends BaseCounterOperation<Long> {
   private static final Log commonsLog = LogFactory.getLog(SetOperation.class, Log.class);

   private final long value;

   public SetOperation(OperationContext operationContext, String counterName, long value, boolean useConsistentHash) {
      super(operationContext, COUNTER_GET_AND_SET_REQUEST, COUNTER_GET_AND_SET_RESPONSE, counterName, useConsistentHash);
      this.value = value;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 8);
      buf.writeLong(value);
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
         if (value > 0) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
