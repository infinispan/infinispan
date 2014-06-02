package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class StatsOperation extends RetryOnFailureOperation<Map<String, String>> {

   public StatsOperation(Codec codec, TransportFactory transportFactory,
            byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, cacheName, topologyId, flags);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers);
   }

   @Override
   protected HeaderParams writeRequest(Transport transport) {
      return writeHeader(transport, STATS_REQUEST);
   }

   @Override
   protected Map<String, String> readResponse(Transport transport, HeaderParams params) {
      readHeaderAndValidate(transport, params);
      int nrOfStats = transport.readVInt();

      Map<String, String> result = new HashMap<String, String>(nrOfStats);
      for (int i = 0; i < nrOfStats; i++) {
         String statName = transport.readString();
         String statValue = transport.readString();
         result.put(statName, statValue);
      }
      return result;
   }
}
