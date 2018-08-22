package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Reads more keys at a time. Specified <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">here</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class BulkGetOperation<K, V> extends StatsAffectingRetryingOperation<Map<K, V>> {
   private final int entryCount;
   private final Map<K, V> result = new HashMap<>();

   public BulkGetOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicInteger topologyId,
                           int flags, Configuration cfg, int entryCount, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(BULK_GET_REQUEST, BULK_GET_RESPONSE, codec, channelFactory, cacheName, topologyId, flags, cfg, dataFormat, clientStatistics);
      this.entryCount = entryCount;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(entryCount));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, entryCount);
      channel.writeAndFlush(buf);
   }

   @Override
   protected void reset() {
      super.reset();
      result.clear();
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      while (buf.readUnsignedByte() == 1) { //there's more!
         K key = bytes2obj(channelFactory.getMarshaller(), ByteBufUtil.readArray(buf), dataFormat.isObjectStorage(), cfg.getClassWhiteList());
         V value = bytes2obj(channelFactory.getMarshaller(), ByteBufUtil.readArray(buf), dataFormat.isObjectStorage(), cfg.getClassWhiteList());
         result.put(key, value);
         decoder.checkpoint();
      }
      complete(result);
   }
}
