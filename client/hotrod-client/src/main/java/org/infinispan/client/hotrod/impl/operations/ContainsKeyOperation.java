package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ContainsKeyOperation extends AbstractKeyOperation<Boolean> {

   public ContainsKeyOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes,
                               byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                               DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(CONTAINS_KEY_REQUEST, CONTAINS_KEY_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
            flags, cfg, dataFormat, clientStatistics, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status));
   }
}
