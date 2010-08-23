package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class StatsOperation extends RetryOnFailureOperation {

   public StatsOperation(TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, cacheName, topologyId, flags);
   }

   @Override
   protected Transport getTransport(int retryCount) {
      return transportFactory.getTransport();
   }

   @Override
   protected Object executeOperation(Transport transport) {
      Map<String, String> result;
      // 1) write header
      long messageId = writeHeader(transport, STATS_REQUEST);
      readHeaderAndValidate(transport, messageId, STATS_RESPONSE);
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
