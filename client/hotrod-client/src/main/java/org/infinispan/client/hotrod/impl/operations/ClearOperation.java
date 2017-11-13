package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to clear operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ClearOperation extends RetryOnFailureOperation<Void> {

   public ClearOperation(Codec codec, ChannelFactory channelFactory,
                         byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel, CLEAR_REQUEST);
   }

   @Override
   public Void decodePayload(ByteBuf buf, short status) {
      return null;
   }
}
