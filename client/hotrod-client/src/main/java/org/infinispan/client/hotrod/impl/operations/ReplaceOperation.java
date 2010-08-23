package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
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
public class ReplaceOperation extends AbstractKeyValueOperation {

   public ReplaceOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId,
                           Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      byte[] result = null;
      short status = sendPutOperation(transport, REPLACE_REQUEST, REPLACE_RESPONSE);
      if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
         result = returnPossiblePrevValue(transport);
      }
      return result;
   }
}
