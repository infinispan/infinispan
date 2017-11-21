package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

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

   public GetKeyMultimapOperation(Codec codec, TransportFactory transportFactory,
                                  Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                                  Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected Collection<V> executeOperation(Transport transport) {
      short status = sendKeyOperation(keyBytes, transport, GET_MULTIMAP_REQUEST, GET_MULTIMAP_RESPONSE);
      Collection<V> result;
      if (HotRodConstants.isNotExist(status)) {
         result = Collections.emptySet();
      } else {
         int size = transport.readVInt();
         result = new HashSet<>(size);
         for (int i = 0; i < size; ++i) {
            V value = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
            result.add(value);
         }
      }
      return result;
   }
}
