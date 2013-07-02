package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveIfUnmodifiedOperation extends AbstractKeyOperation<VersionedOperationResponse> {

   private final long version;

   public RemoveIfUnmodifiedOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags, long version) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
      this.version = version;
   }

   @Override
   protected VersionedOperationResponse executeOperation(Transport transport) {
      // 1) write header
      HeaderParams params = writeHeader(transport, REMOVE_IF_UNMODIFIED_REQUEST);

      //2) write message body
      transport.writeArray(key);
      transport.writeLong(version);
      transport.flush();

      //process response and return
      return returnVersionedOperationResponse(transport, params);
   }
}
