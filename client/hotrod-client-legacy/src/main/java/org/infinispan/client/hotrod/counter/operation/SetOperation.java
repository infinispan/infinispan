package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
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

   private final long value;

   public SetOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId,
                       Configuration cfg, String counterName, long value, boolean useConsistentHash) {
      super(COUNTER_GET_AND_SET_REQUEST, COUNTER_GET_AND_SET_RESPONSE, channelFactory, topologyId, cfg, counterName, useConsistentHash);
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
            throw Log.HOTROD.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw Log.HOTROD.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
