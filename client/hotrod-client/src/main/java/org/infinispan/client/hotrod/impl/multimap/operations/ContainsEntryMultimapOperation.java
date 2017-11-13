package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "contains entry" for multimap as defined by <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#hot_rod_protocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class ContainsEntryMultimapOperation extends AbstractMultimapKeyValueOperation<Boolean> {

   public ContainsEntryMultimapOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg, byte[] value) {
      super(codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value);
   }

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(CONTAINS_ENTRY_REQUEST);
      scheduleRead(channel, header);
      sendKeyValueOperation(channel, header);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;

   }
}
