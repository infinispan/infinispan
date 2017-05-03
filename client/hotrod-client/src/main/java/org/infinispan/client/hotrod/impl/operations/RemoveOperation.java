package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implement "remove" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveOperation<V> extends AbstractKeyOperation<V> {

   public RemoveOperation(Codec codec, TransportFactory transportFactory,
         Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   public V executeOperation(Transport transport) {
      short status = sendKeyOperation(keyBytes, transport, REMOVE_REQUEST, REMOVE_RESPONSE);
      V result = returnPossiblePrevValue(transport, status);
      if (HotRodConstants.isNotExist(status))
         return null;

      return result; // NO_ERROR_STATUS
   }
}
