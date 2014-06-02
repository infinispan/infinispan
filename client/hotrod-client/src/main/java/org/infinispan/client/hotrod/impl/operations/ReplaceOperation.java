package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

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
   protected HeaderParams writeRequest(Transport transport) {
      return writeKeyValueRequest(transport, REPLACE_REQUEST);
   }

   @Override
   protected byte[] readResponse(Transport transport, HeaderParams params) {
      short status = readHeaderAndValidate(transport, params);
      if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
         return returnPossiblePrevValue(transport);
      }
      return null;
   }
}
