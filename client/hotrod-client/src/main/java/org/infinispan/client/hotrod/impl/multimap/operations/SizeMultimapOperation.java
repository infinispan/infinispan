package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "size" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class SizeMultimapOperation extends RetryOnFailureOperation<Long> {

   protected SizeMultimapOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Long executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, SIZE_MULTIMAP_REQUEST);
      transport.flush();
      readHeaderAndValidate(transport, params);
      return transport.readVLong();
   }

   @Override
   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }
}
