package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "get" operation as described by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetOperation<V> extends AbstractKeyOperation<V> {

   public GetOperation(Codec codec, ChannelFactory channelFactory,
                       Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags,
                       Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(GET_REQUEST, GET_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology, flags, cfg,
            dataFormat, clientStatistics, null);
   }

   @Override
   public void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status)) {
         statsDataRead(true);
         complete(dataFormat().valueToObj(ByteBufUtil.readArray(buf), cfg.getClassAllowList()));
      } else {
         statsDataRead(false);
         complete(null);
      }
   }
}
