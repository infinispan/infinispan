package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

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
public class GetAllOperation<K, V> extends StatsAffectingRetryingOperation<Map<K, V>> {

   private Map<K, V> result;
   private int size = -1;

   public GetAllOperation(Codec codec, ChannelFactory channelFactory,
                          Set<byte[]> keys, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                          int flags, Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(GET_ALL_REQUEST, GET_ALL_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat,
            clientStatistics, null);
      this.keys = keys;
   }

   protected final Set<byte[]> keys;

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

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
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
   }

   @Override
   protected void reset() {
      super.reset();
      result = null;
      size = -1;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(keys.iterator().next(), failedServers, cacheName(), this);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (size < 0) {
         size = ByteBufUtil.readVInt(buf);
         result = new HashMap<>(size);
         decoder.checkpoint();
      }
      while (result.size() < size) {
         K key = dataFormat().keyToObj(ByteBufUtil.readArray(buf), cfg.getClassAllowList());
         V value = dataFormat().valueToObj(ByteBufUtil.readArray(buf), cfg.getClassAllowList());
         result.put(key, value);
         decoder.checkpoint();
      }
      statsDataRead(true, size);
      statsDataRead(false, keys.size() - size);
      complete(result);
   }
}
