package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_RESPONSE;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "size" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class SizeMultimapOperation extends RetryOnFailureOperation<Long> {

   private final boolean supportsDuplicates;

   protected SizeMultimapOperation(OperationContext operationContext, CacheOptions options, boolean supportsDuplicates) {
      super(operationContext, SIZE_MULTIMAP_REQUEST, SIZE_MULTIMAP_RESPONSE, options, null);
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
   protected void sendHeader(Channel channel) {
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + codec.estimateSizeMultimapSupportsDuplicated());
      operationContext.getCodec().writeHeader(buf, header);
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
      channel.writeAndFlush(buf);
   }
}
