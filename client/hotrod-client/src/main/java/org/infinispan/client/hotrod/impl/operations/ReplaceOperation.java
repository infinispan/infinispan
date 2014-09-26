package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "Replace" operation as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ReplaceOperation extends AbstractKeyValueOperation<byte[]> {

   public ReplaceOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId,
            Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected byte[] executeOperation(Transport transport) {
      byte[] result = null;
      short status = sendPutOperation(transport, REPLACE_REQUEST, REPLACE_RESPONSE);
      result = returnPossiblePrevValue(transport, status);
      return result;
   }
}
