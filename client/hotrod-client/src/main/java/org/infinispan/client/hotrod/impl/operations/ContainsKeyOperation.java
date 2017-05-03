package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ContainsKeyOperation extends AbstractKeyOperation<Boolean> {

   public ContainsKeyOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes,
                               byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, key, keyBytes,cacheName, topologyId, flags, cfg);
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      boolean containsKey = false;
      short status = sendKeyOperation(keyBytes, transport, CONTAINS_KEY_REQUEST, CONTAINS_KEY_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         containsKey = false;
      } else if (HotRodConstants.isSuccess(status)) {
         containsKey = true;
      }
      return containsKey;
   }
}
