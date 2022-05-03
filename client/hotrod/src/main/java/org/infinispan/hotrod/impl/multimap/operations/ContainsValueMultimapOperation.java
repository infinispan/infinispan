package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_RESPONSE;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "contains value" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @since 14.0
 */
public class ContainsValueMultimapOperation extends RetryOnFailureOperation<Boolean> {

   protected final byte[] value;
   protected ContainsValueMultimapOperation(OperationContext operationContext, int flags,
                                            byte[] value, CacheOptions options) {
      super(operationContext, CONTAINS_VALUE_MULTIMAP_REQUEST, CONTAINS_VALUE_MULTIMAP_RESPONSE, options, null);
      this.value = value;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Boolean.FALSE);
      } else {
         complete(buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE);
      }
   }

   protected void sendValueOperation(Channel channel) {
      CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) +
            codec.estimateExpirationSize(expiration) +
            ByteBufUtil.estimateArraySize(value));
      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, expiration);
      ByteBufUtil.writeArray(buf, value);
      channel.writeAndFlush(buf);
   }
}
