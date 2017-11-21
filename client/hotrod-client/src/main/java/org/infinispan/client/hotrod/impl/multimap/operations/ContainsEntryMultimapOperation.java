package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_RESPONSE;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

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

   public ContainsEntryMultimapOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg, byte[] value) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value);
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      short status = sendKeyValueOperation(transport, CONTAINS_ENTRY_REQUEST, CONTAINS_ENTRY_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return transport.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }
}
