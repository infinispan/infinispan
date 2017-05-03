package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implement "replaceIfUnmodified" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ReplaceIfUnmodifiedOperation extends AbstractKeyValueOperation<VersionedOperationResponse> {
   private final long version;

   public ReplaceIfUnmodifiedOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName,
                                       AtomicInteger topologyId, int flags, Configuration cfg, byte[] value,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      this.version = version;
   }

   @Override
   protected VersionedOperationResponse executeOperation(Transport transport) {
      // 1) write header
      HeaderParams params = writeHeader(transport, REPLACE_IF_UNMODIFIED_REQUEST);

      //2) write message body
      transport.writeArray(keyBytes);
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeLong(version);
      transport.writeArray(value);
      transport.flush();

      return returnVersionedOperationResponse(transport, params);
   }
}
