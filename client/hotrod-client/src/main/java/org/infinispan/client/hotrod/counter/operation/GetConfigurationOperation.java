package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.decodeConfiguration;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
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

   public GetConfigurationOperation(ChannelFactory channelFactory, AtomicReference<ClientTopology> topologyId,
                                    Configuration cfg, String counterName) {
      super(COUNTER_GET_CONFIGURATION_REQUEST, COUNTER_GET_CONFIGURATION_RESPONSE, channelFactory, topologyId, cfg, counterName, false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndCounterNameAndRead(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeHeaderAndCounterName(buf);
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
