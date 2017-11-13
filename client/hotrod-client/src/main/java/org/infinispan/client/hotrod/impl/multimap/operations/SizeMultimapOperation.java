package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "size" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class SizeMultimapOperation extends RetryOnFailureOperation<Long> {

   protected SizeMultimapOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel, SIZE_MULTIMAP_REQUEST);
   }

   @Override
   public Long decodePayload(ByteBuf buf, short status) {
      return ByteBufUtil.readVLong(buf);
   }
}
