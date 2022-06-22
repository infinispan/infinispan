package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_RESPONSE;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

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

   public ContainsEntryMultimapOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes,
                                         byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                         byte[] value, ClientStatistics clientStatistics, boolean supportsDuplicates) {
      super(CONTAINS_ENTRY_REQUEST, CONTAINS_ENTRY_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
            flags, cfg, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, null, clientStatistics, supportsDuplicates);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Boolean.FALSE);
      } else {
         complete(buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE);
      }
   }
}
