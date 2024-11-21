package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_RESPONSE;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

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

   private final boolean supportsDuplicates;

   protected SizeMultimapOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, boolean supportsDuplicates) {
      super(SIZE_MULTIMAP_REQUEST, SIZE_MULTIMAP_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg,
            null, null);
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(ByteBufUtil.readVLong(buf));
   }

   @Override
   protected void sendHeaderAndRead(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
   }

   @Override
   protected void sendHeader(Channel channel) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + codec.estimateSizeMultimapSupportsDuplicated());
      codec.writeHeader(buf, header);
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
      channel.writeAndFlush(buf);
   }
}
