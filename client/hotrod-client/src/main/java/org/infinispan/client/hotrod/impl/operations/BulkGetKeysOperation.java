package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Reads all keys. Similar to <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">BulkGet</a>, but without the entry values.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
public class BulkGetKeysOperation<K> extends RetryOnFailureOperation<Set<K>> {
   private final int scope;

   public BulkGetKeysOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName,
                               AtomicInteger topologyId, int flags, Configuration cfg, int scope) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
      this.scope = scope;
   }
   
   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Set<K> executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, BULK_GET_KEYS_REQUEST);
      transport.writeVInt(scope);
      transport.flush();
      short status = readHeaderAndValidate(transport, params);
      Set<K> result = new HashSet<K>();
      while ( transport.readByte() == 1) { //there's more!
         result.add(codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist()));
      }
      return result;
   }
}
