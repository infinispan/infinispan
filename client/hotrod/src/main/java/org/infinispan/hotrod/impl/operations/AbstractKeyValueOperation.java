package org.infinispan.hotrod.impl.operations;

import static org.infinispan.commons.util.Util.printArray;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @since 14.0
 */
public abstract class AbstractKeyValueOperation<K, T> extends AbstractKeyOperation<K, T> {

   protected final byte[] value;

   protected AbstractKeyValueOperation(OperationContext operationContext, short requestCode, short responseCode, K key, byte[] keyBytes,
                                       byte[] value,
                                       CacheOptions options,
                                       DataFormat dataFormat) {
      super(operationContext, requestCode, responseCode, key, keyBytes, options, dataFormat);
      this.value = value;
   }

   protected void sendKeyValueOperation(Channel channel) {
      Codec codec = operationContext.getCodec();
      CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + keyBytes.length +
            codec.estimateExpirationSize(expiration) + value.length);
      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, expiration);
      ByteBufUtil.writeArray(buf, value);
      channel.writeAndFlush(buf);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      super.addParams(sb);
      sb.append(", value=").append(printArray(value));
   }
}
