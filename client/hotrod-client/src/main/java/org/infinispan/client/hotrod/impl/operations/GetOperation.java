package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "get" operation as described by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetOperation extends AbstractKeyOperation<byte[]> {

   public GetOperation(Codec codec, TransportFactory transportFactory,
         byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected HeaderParams writeRequest(Transport transport) {
      return writeKeyRequest(transport, GET_REQUEST);
   }

   @Override
   protected byte[] readResponse(Transport transport, HeaderParams params) {
      short status = readHeaderAndValidate(transport, params);
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         return null;
      } else {
         if (status == NO_ERROR_STATUS) {
            return transport.readArray();
         }
         return null;
      }
   }
}
