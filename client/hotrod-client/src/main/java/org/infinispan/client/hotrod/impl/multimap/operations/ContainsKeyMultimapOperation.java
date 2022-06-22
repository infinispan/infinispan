package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_KEY_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_KEY_MULTIMAP_RESPONSE;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Implements "contains key" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ContainsKeyMultimapOperation extends AbstractMultimapKeyOperation<Boolean> {
   public ContainsKeyMultimapOperation(Codec codec, ChannelFactory transportFactory, Object key, byte[] keyBytes,
                                       byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg,
                                       ClientStatistics clientStatistics, boolean supportsDuplicates) {
      super(CONTAINS_KEY_MULTIMAP_REQUEST, CONTAINS_KEY_MULTIMAP_RESPONSE, codec, transportFactory, key, keyBytes, cacheName,
            clientTopology, flags, cfg, null, clientStatistics, supportsDuplicates);
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
