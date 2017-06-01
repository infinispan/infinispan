package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class StatsOperation extends RetryOnFailureOperation<Map<String, String>> {

   public StatsOperation(Codec codec, TransportFactory transportFactory,
                         byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Map<String, String> executeOperation(Transport transport) {
      Map<String, String> result;
      // 1) write header
      HeaderParams params = writeHeader(transport, STATS_REQUEST);
      transport.flush();

      readHeaderAndValidate(transport, params);
      int nrOfStats = transport.readVInt();

      result = new HashMap<String, String>();
      for (int i = 0; i < nrOfStats; i++) {
         String statName = transport.readString();
         String statValue = transport.readString();
         result.put(statName, statValue);
      }
      return result;
   }
}
