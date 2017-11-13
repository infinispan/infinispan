package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_KEY_MULTIMAP_REQUEST;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "contains key" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ContainsKeyMultimapOperation extends AbstractMultimapKeyOperation<Boolean> {
   public ContainsKeyMultimapOperation(Codec codec, ChannelFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   public void executeOperation(Channel channel) {
      HeaderParams header = headerParams(CONTAINS_KEY_MULTIMAP_REQUEST);
      scheduleRead(channel, header);
      sendArrayOperation(channel, header, keyBytes);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }
}
