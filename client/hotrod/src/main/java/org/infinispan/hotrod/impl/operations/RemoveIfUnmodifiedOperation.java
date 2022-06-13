package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.VersionedOperationResponse;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @since 14.0
 */
public class RemoveIfUnmodifiedOperation<K, V> extends AbstractKeyOperation<K, VersionedOperationResponse<CacheEntry<K, V>>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(OperationContext operationContext,
         K key, byte[] keyBytes, long version, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, REMOVE_IF_UNMODIFIED_REQUEST, REMOVE_IF_UNMODIFIED_RESPONSE, key, keyBytes, options, dataFormat.withoutValueType());
      this.version = version;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) + 8);
      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      buf.writeLong(version);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(returnVersionedOperationResponse(buf, status));
   }
}
