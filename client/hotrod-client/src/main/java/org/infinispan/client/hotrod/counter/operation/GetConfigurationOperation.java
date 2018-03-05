package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.decodeConfiguration;

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
 * A counter configuration for {@link CounterManager#getConfiguration(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetConfigurationOperation extends BaseCounterOperation<CounterConfiguration> {

   public GetConfigurationOperation(Codec codec, ChannelFactory channelFactory, AtomicInteger topologyId,
                                    Configuration cfg, String counterName) {
      super(COUNTER_GET_CONFIGURATION_REQUEST, COUNTER_GET_CONFIGURATION_RESPONSE, codec, channelFactory, topologyId, cfg, counterName);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel, COUNTER_GET_CONFIGURATION_REQUEST);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (status != NO_ERROR_STATUS) {
         complete(null);
         return;
      }

      complete(decodeConfiguration(buf::readByte, buf::readLong, () -> ByteBufUtil.readVInt(buf)));
   }
}
