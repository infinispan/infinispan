package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "get" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class GetKeyMultimapOperation<V> extends AbstractKeyOperation<Collection<V>> {
   private int size;
   private Collection<V> result;

   public GetKeyMultimapOperation(Codec codec, ChannelFactory channelFactory,
                                  Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                                  Configuration cfg) {
      super(GET_MULTIMAP_REQUEST, GET_MULTIMAP_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   protected void reset() {
      super.reset();
      result = null;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Collections.emptySet());
         return;
      } else if (result == null) {
         size = ByteBufUtil.readVInt(buf);
         result = new HashSet<>(size);
      }
      while (result.size() < size) {
         V value = codec.readUnmarshallByteArray(buf, status, cfg.serialWhitelist(), channelFactory.getMarshaller());
         result.add(value);
         decoder.checkpoint();
      }
      complete(result);
   }
}
