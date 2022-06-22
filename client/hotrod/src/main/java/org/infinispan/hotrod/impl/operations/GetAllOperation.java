package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "getAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public class GetAllOperation<K, V> extends StatsAffectingRetryingOperation<Map<K, V>> {

   protected final Set<byte[]> keys;
   private Map<K, V> result;
   private int size = -1;

   public GetAllOperation(OperationContext operationContext,
                          Set<byte[]> keys, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, GET_ALL_REQUEST, GET_ALL_RESPONSE, options, dataFormat);
      this.keys = keys;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      int bufSize = operationContext.getCodec().estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(keys.size());
      for (byte[] key : keys) {
         bufSize += ByteBufUtil.estimateArraySize(key);
      }
      ByteBuf buf = channel.alloc().buffer(bufSize);

      operationContext.getCodec().writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
      channel.writeAndFlush(buf);
   }

   @Override
   protected void reset() {
      super.reset();
      result = null;
      size = -1;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      operationContext.getChannelFactory().fetchChannelAndInvoke(keys.iterator().next(), failedServers, operationContext.getCacheNameBytes(), this);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (size < 0) {
         size = ByteBufUtil.readVInt(buf);
         result = new HashMap<>(size);
         decoder.checkpoint();
      }
      while (result.size() < size) {
         K key = dataFormat().keyToObj(ByteBufUtil.readArray(buf), operationContext.getConfiguration().getClassAllowList());
         V value = dataFormat().valueToObj(ByteBufUtil.readArray(buf), operationContext.getConfiguration().getClassAllowList());
         result.put(key, value);
         decoder.checkpoint();
      }
      statsDataRead(true, size);
      statsDataRead(false, keys.size() - size);
      complete(result);
   }
}
