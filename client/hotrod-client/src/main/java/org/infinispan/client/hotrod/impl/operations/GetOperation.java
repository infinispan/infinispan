package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "get" operation as described by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetOperation<V> extends AbstractKeyOperation<V> {

   public GetOperation(Codec codec, TransportFactory transportFactory,
                       Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                       Configuration cfg) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   public V executeOperation(Transport transport) {
      V result = null;
      short status = sendKeyOperation(keyBytes, transport, GET_REQUEST, GET_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         result = null;
      } else {
         if (HotRodConstants.isSuccess(status)) {
            result = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
         }
      }
      return result;
   }
}
