package org.infinispan.client.hotrod.counter.operation;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A set operation for {@link StrongCounter#getAndSet(long)}
 *
 * @author Dipanshu Gupta
 * @since 15
 */
public class SetOperation extends BaseCounterOperation<Long> {
   private static final Log commonsLog = LogFactory.getLog(SetOperation.class, Log.class);

   private final long value;

   public SetOperation(String counterName, long value, boolean useConsistentHash) {
      super(counterName, useConsistentHash);
      this.value = value;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      buf.writeLong(value);
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
         if (value > 0) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }

   @Override
   public short requestOpCode() {
      return COUNTER_GET_AND_SET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_GET_AND_SET_RESPONSE;
   }
}
