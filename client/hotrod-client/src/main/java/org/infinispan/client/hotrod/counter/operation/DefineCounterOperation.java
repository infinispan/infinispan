package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter define operation for {@link CounterManager#defineCounter(String, CounterConfiguration)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class DefineCounterOperation extends BaseCounterOperation<Boolean> {

   private final CounterConfiguration configuration;

   public DefineCounterOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
                                 Configuration cfg, String counterName, CounterConfiguration configuration) {
      super(COUNTER_CREATE_REQUEST, COUNTER_CREATE_RESPONSE, codec, channelFactory, topologyId, cfg, counterName);
      this.configuration = configuration;
   }

   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, 28);
      encodeConfiguration(configuration, buf::writeByte, buf::writeLong, i -> ByteBufUtil.writeVInt(buf, i));
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      complete(status == NO_ERROR_STATUS);
   }
}
