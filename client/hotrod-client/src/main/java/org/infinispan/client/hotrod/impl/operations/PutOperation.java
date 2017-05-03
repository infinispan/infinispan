package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutOperation<V> extends AbstractKeyValueOperation<V> {

   public PutOperation(Codec codec, TransportFactory transportFactory,
                       Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                       int flags, Configuration cfg, byte[] value, long lifespan, TimeUnit lifespanTimeUnit,
                       long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId,
         flags, cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   protected V executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_REQUEST, PUT_RESPONSE);
      if (!HotRodConstants.isSuccess(status)) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      return returnPossiblePrevValue(transport, status);
   }
}
