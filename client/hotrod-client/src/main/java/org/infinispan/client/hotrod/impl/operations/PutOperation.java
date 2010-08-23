package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.jgroups.annotations.Immutable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutOperation extends AbstractKeyValueOperation {

   public PutOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId,
                       Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_REQUEST, PUT_RESPONSE);
      if (status != NO_ERROR_STATUS) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      return returnPossiblePrevValue(transport);
   }
}
