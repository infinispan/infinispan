package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implements "get" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class GetKeyMultimapOperation<V> extends AbstractMultimapKeyOperation<Collection<V>> {
   private int size;
   private Collection<V> result;

   public GetKeyMultimapOperation(Codec codec, ChannelFactory channelFactory,
                                  Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags,
                                  Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics, boolean supportsDuplicates) {
      super(GET_MULTIMAP_REQUEST, GET_MULTIMAP_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
            flags, cfg, dataFormat, clientStatistics, supportsDuplicates);
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
         result = supportsDuplicates ? new ArrayList<>(size) : new HashSet<>(size);
      }
      while (result.size() < size) {
         V value = bytes2obj(channelFactory.getMarshaller(), ByteBufUtil.readArray(buf), dataFormat().isObjectStorage(), cfg.getClassAllowList());
         result.add(value);
         decoder.checkpoint();
      }
      complete(result);
   }
}
