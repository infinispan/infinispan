package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "Replace" operation as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ReplaceOperation<V> extends AbstractKeyValueOperation<V> {

   public ReplaceOperation(Codec codec, TransportFactory transportFactory,
                           Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                           int flags, Configuration cfg, byte[] value,
                           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value,
            lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   protected V executeOperation(Transport transport) {
      short status = sendPutOperation(transport, REPLACE_REQUEST, REPLACE_RESPONSE);
      return returnPossiblePrevValue(transport, status);
   }
}
