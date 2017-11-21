package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_RESPONSE;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "remove" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class RemoveKeyMultimapOperation extends AbstractMultimapKeyOperation<Boolean> {
   public RemoveKeyMultimapOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   public Boolean executeOperation(Transport transport) {
      short status = sendKeyOperation(keyBytes, transport, REMOVE_KEY_MULTIMAP_REQUEST, REMOVE_KEY_MULTIMAP_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return transport.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }
}
