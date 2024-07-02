package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
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

   public AddOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId, Configuration cfg,
         String counterName, long delta, boolean useConsistentHash) {
      super(COUNTER_ADD_AND_GET_REQUEST, COUNTER_ADD_AND_GET_RESPONSE, channelFactory, topologyId, cfg, counterName, useConsistentHash);
      this.delta = delta;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 8);
      buf.writeLong(delta);
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeHeaderAndCounterName(buf);
      buf.writeLong(delta);
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
