package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
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
public class RemoveOperation extends AbstractKeyOperation<byte[]> {

   public RemoveOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   public byte[] executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, REMOVE_REQUEST, REMOVE_RESPONSE);
      byte[] result = returnPossiblePrevValue(transport, status);
      if (status == KEY_DOES_NOT_EXIST_STATUS)
         return null;

      return result; // NO_ERROR_STATUS
   }
}
