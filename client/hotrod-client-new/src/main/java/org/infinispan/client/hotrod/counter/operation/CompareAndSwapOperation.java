package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A compare-and-set operation for {@link StrongCounter#compareAndSwap(long, long)} and {@link
 * StrongCounter#compareAndSet(long, long)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CompareAndSwapOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(CompareAndSwapOperation.class, Log.class);

   private final long expect;
   private final long update;
   private final CounterConfiguration counterConfiguration;

   public CompareAndSwapOperation(String counterName, long expect, long update, CounterConfiguration counterConfiguration) {
      super(counterName, true);
      this.expect = expect;
      this.update = update;
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      buf.writeLong(expect);
      buf.writeLong(update);
   }

   @Override
   public Long createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      return buf.readLong();
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

   @Override
   public short requestOpCode() {
      return COUNTER_CAS_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_CAS_RESPONSE;
   }
}
