package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_RESPONSE;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractCacheOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
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
public class SizeMultimapOperation extends AbstractCacheOperation<Long> {

   private final boolean supportsDuplicates;

   protected SizeMultimapOperation(InternalRemoteCache<?, ?> remoteCache, boolean supportsDuplicates) {
      super(remoteCache);
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
   }

   @Override
   public Long createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return ByteBufUtil.readVLong(buf);
   }

   @Override
   public short requestOpCode() {
      return SIZE_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return SIZE_MULTIMAP_RESPONSE;
   }
}
