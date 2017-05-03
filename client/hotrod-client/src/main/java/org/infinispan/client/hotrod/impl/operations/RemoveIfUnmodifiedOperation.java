package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.configuration.Configuration;
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
public class RemoveIfUnmodifiedOperation<V> extends AbstractKeyOperation<VersionedOperationResponse<V>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(Codec codec, TransportFactory transportFactory,
                                      Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                                      int flags, Configuration cfg,
                                      long version) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
      this.version = version;
   }

   @Override
   protected VersionedOperationResponse<V> executeOperation(Transport transport) {
      // 1) write header
      HeaderParams params = writeHeader(transport, REMOVE_IF_UNMODIFIED_REQUEST);

      //2) write message body
      transport.writeArray(keyBytes);
      transport.writeLong(version);
      transport.flush();

      //process response and return
      return returnVersionedOperationResponse(transport, params);
   }
}
