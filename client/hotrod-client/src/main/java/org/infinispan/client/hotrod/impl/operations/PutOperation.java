package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutOperation extends AbstractKeyValueOperation<byte[]> {

   public PutOperation(Codec codec, TransportFactory transportFactory,
                       byte[] key, byte[] cacheName, AtomicInteger topologyId,
                       Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected byte[] executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_REQUEST, PUT_RESPONSE);
      if (status != NO_ERROR_STATUS && status != SUCCESS_WITH_PREVIOUS) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      return returnPossiblePrevValue(transport, status);
   }
}
