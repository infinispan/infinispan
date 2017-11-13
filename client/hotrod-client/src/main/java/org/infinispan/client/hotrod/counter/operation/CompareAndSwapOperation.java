package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
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

   public CompareAndSwapOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
                                  Configuration cfg, String counterName, long expect, long update, CounterConfiguration counterConfiguration) {
      super(codec, channelFactory, topologyId, cfg, counterName);
      this.expect = expect;
      this.update = update;
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, COUNTER_CAS_REQUEST, 16);
      buf.writeLong(expect);
      buf.writeLong(update);
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
         if (update >= counterConfiguration.upperBound()) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
