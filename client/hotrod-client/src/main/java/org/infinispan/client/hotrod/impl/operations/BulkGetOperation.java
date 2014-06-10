package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Reads more keys at a time. Specified <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">here</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class BulkGetOperation extends RetryOnFailureOperation<Map<byte[], byte[]>> {

   private final int entryCount;

   public BulkGetOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags, int entryCount) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.entryCount = entryCount;
   }
   
   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers);
   }

   @Override
   protected HeaderParams writeRequest(Transport transport) {
      HeaderParams params = writeHeader(transport, BULK_GET_REQUEST);
      transport.writeVInt(entryCount);
      return params;
   }

   @Override
   protected Map<byte[], byte[]> readResponse(Transport transport, HeaderParams params) {
      readHeaderAndValidate(transport, params);
      Map<byte[], byte[]> result = new HashMap<byte[], byte[]>();
      while ( transport.readByte() == 1) { //there's more!
         result.put(transport.readArray(), transport.readArray());
      }
      return result;
   }
}
