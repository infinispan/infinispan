package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.exceptions.InvalidResponseException;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "putAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public class PutAllOperation extends StatsAffectingRetryingOperation<Void> {

   protected final Map<byte[], byte[]> map;

   public PutAllOperation(OperationContext operationContext,
                          Map<byte[], byte[]> map, CacheWriteOptions options,
                          DataFormat dataFormat) {
      super(operationContext, PUT_ALL_REQUEST, PUT_ALL_RESPONSE, options, dataFormat);
      this.map = map;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      CacheEntryExpiration.Impl expiration = (CacheEntryExpiration.Impl) ((CacheWriteOptions) options).expiration();
      int bufSize = codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(map.size()) +
            codec.estimateExpirationSize(expiration);
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         bufSize += ByteBufUtil.estimateArraySize(entry.getKey());
         bufSize += ByteBufUtil.estimateArraySize(entry.getValue());
      }
      ByteBuf buf = channel.alloc().buffer(bufSize);

      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, expiration);
      ByteBufUtil.writeVInt(buf, map.size());
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         ByteBufUtil.writeArray(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
      channel.writeAndFlush(buf);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      operationContext.getChannelFactory().fetchChannelAndInvoke(map.keySet().iterator().next(), failedServers, operationContext.getCacheNameBytes(), this);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         statsDataStore(map.size());
         complete(null);
         return;
      }
      throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
   }
}
