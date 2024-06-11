package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.exception.CounterOutOfBoundsException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Add operation.
 * <p>
 * Adds the {@code delta} to the counter's value and returns the result.
 * <p>
 * It can throw a {@link CounterOutOfBoundsException} if the counter is bounded and the it has been reached.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class AddOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(AddOperation.class, Log.class);

   private final long delta;

   public AddOperation(String counterName, long delta, boolean useConsistentHash) {
      super(counterName, useConsistentHash);
      this.delta = delta;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      buf.writeLong(delta);
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

   @Override
   public Long createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      checkStatus(status);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      return buf.readLong();
   }

   @Override
   public short requestOpCode() {
      return COUNTER_ADD_AND_GET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_ADD_AND_GET_RESPONSE;
   }
}
