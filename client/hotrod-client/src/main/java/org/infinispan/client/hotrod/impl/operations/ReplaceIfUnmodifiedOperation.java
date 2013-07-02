package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

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

   public ReplaceIfUnmodifiedOperation(Codec codec, TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                       AtomicInteger topologyId, Flag[] flags, byte[] value, int lifespan,
                                       int maxIdle, long version) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
      this.version = version;
   }

   @Override
   protected VersionedOperationResponse executeOperation(Transport transport) {
      // 1) write header
      HeaderParams params = writeHeader(transport, REPLACE_IF_UNMODIFIED_REQUEST);

      //2) write message body
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeLong(version);
      transport.writeArray(value);
      transport.flush();

      return returnVersionedOperationResponse(transport, params);
   }
}
