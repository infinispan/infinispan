package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.VersionedOperationResponse;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implement "replaceIfUnmodified" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class ReplaceIfUnmodifiedOperation<K, V> extends AbstractKeyValueOperation<K, VersionedOperationResponse<CacheEntry<K, V>>> {
   private final long version;

   public ReplaceIfUnmodifiedOperation(OperationContext operationContext, K key, byte[] keyBytes,
         byte[] value,
         long version, CacheWriteOptions options, DataFormat dataFormat) {
      super(operationContext, REPLACE_IF_UNMODIFIED_REQUEST, REPLACE_IF_UNMODIFIED_RESPONSE, key, keyBytes, value, options, dataFormat);
      this.version = version;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) +
            codec.estimateExpirationSize(expiration) + 8 +
            ByteBufUtil.estimateArraySize(value));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, expiration);
      buf.writeLong(version);
      ByteBufUtil.writeArray(buf, value);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         statsDataStore();
      }
      complete(returnVersionedOperationResponse(buf, status));
   }
}
