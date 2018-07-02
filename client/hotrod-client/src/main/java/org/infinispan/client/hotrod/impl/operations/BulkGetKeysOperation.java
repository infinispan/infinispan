package org.infinispan.client.hotrod.impl.operations;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Reads all keys. Similar to <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">BulkGet</a>, but without the entry values.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
public class BulkGetKeysOperation<K> extends RetryOnFailureOperation<Set<K>> {
   private final int scope;
   private final Set<K> result = new HashSet<>();

   public BulkGetKeysOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName,
                               AtomicInteger topologyId, int flags, Configuration cfg, int scope) {
      super(BULK_GET_KEYS_REQUEST, BULK_GET_KEYS_RESPONSE, codec, channelFactory, cacheName, topologyId, flags, cfg, null);
      this.scope = scope;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(scope));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, scope);
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
         result.add(codec.readUnmarshallByteArray(buf, status, cfg.getClassWhiteList(), channelFactory.getMarshaller()));
         decoder.checkpoint();
      }
      complete(result);
   }
}
