package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "getAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author William Burns
 * @since 7.2
 */
@Immutable
public class GetAllOperation<K, V> extends RetryOnFailureOperation<Map<K, V>> {

   private HeaderDecoder<Map<K, V>> decoder;
   private Map<K, V> result;
   private int size = -1;

   public GetAllOperation(Codec codec, ChannelFactory channelFactory,
                          Set<byte[]> keys, byte[] cacheName, AtomicInteger topologyId,
                          int flags, Configuration cfg) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
      this.keys = keys;
   }

   protected final Set<byte[]> keys;

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(HotRodConstants.GET_ALL_REQUEST);
      decoder = scheduleRead(channel, header);

      int bufSize = codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(keys.size());
      for (byte[] key : keys) {
         bufSize += ByteBufUtil.estimateArraySize(key);
      }
      ByteBuf buf = channel.alloc().buffer(bufSize);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
      channel.writeAndFlush(buf);
   }

   @Override
   protected void reset() {
      result = null;
      size = -1;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(keys.iterator().next(), failedServers, cacheName, this);
   }

   @Override
   public Map<K, V> decodePayload(ByteBuf buf, short status) {
      if (size < 0) {
         size = ByteBufUtil.readVInt(buf);
         result = new HashMap<>(size);
         decoder.checkpoint();
      }
      while (result.size() < size) {
         K key = codec.readUnmarshallByteArray(buf, status, cfg.serialWhitelist(), channelFactory.getMarshaller());
         V value = codec.readUnmarshallByteArray(buf, status, cfg.serialWhitelist(), channelFactory.getMarshaller());
         result.put(key, value);
         decoder.checkpoint();
      }
      return result;
   }
}
