package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

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
                               byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, channelFactory, key, keyBytes,cacheName, topologyId, flags, cfg);
   }

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(CONTAINS_KEY_REQUEST);
      scheduleRead(channel, header);
      sendArrayOperation(channel, header, keyBytes);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      return !HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status);
   }
}
