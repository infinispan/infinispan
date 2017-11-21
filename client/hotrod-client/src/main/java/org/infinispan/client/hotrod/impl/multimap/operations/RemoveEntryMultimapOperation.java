package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_RESPONSE;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Implements "remove" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class RemoveEntryMultimapOperation extends AbstractMultimapKeyValueOperation<Boolean> {

   public RemoveEntryMultimapOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg, byte[] value) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value);
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      short status = sendKeyValueOperation(transport, REMOVE_ENTRY_MULTIMAP_REQUEST, REMOVE_ENTRY_MULTIMAP_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return transport.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }
}
