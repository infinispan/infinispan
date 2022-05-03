package org.infinispan.hotrod.impl.counter.operation;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A compare-and-set operation for {@link StrongCounter#compareAndSwap(long, long)} and {@link
 * StrongCounter#compareAndSet(long, long)}.
 *
 * @since 14.0
 */
public class CompareAndSwapOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(CompareAndSwapOperation.class, Log.class);

   private final long expect;
   private final long update;
   private final CounterConfiguration counterConfiguration;

   public CompareAndSwapOperation(OperationContext operationContext, String counterName, long expect, long update, CounterConfiguration counterConfiguration) {
      super(operationContext, COUNTER_CAS_REQUEST, COUNTER_CAS_RESPONSE, counterName, true);
      this.expect = expect;
      this.update = update;
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 16);
      buf.writeLong(expect);
      buf.writeLong(update);
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
         if (update >= counterConfiguration.upperBound()) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
