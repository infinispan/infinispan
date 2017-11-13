package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
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

   public AddOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId, Configuration cfg,
                       String counterName, long delta) {
      super(codec, channelFactory, topologyId, cfg, counterName);
      this.delta = delta;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, COUNTER_ADD_AND_GET_REQUEST, 8);
      buf.writeLong(delta);
      channel.writeAndFlush(buf);
   }

   @Override
   public Long decodePayload(ByteBuf buf, short status) {
      checkStatus(status);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      return buf.readLong();
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
