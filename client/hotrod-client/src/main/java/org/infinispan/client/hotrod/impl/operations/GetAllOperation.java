package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "getAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author William Burns
 * @since 7.2
 */
@Immutable
public class GetAllOperation extends RetryOnFailureOperation<Map<byte[], byte[]>> {

   public GetAllOperation(Codec codec, TransportFactory transportFactory,
                       Set<byte[]> keys, byte[] cacheName, AtomicInteger topologyId,
                       Flag[] flags) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.keys = keys;
   }

   protected final Set<byte[]> keys;

   @Override
   protected Map<byte[], byte[]> executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, GET_ALL_REQUEST);
      transport.writeVInt(keys.size());
      for (byte[] key : keys) {
         transport.writeArray(key);
      }
      transport.flush();

      readHeaderAndValidate(transport, params);
      int size = transport.readVInt();
      Map<byte[], byte[]> result = new HashMap<byte[], byte[]>(size);
      for (int i = 0; i < size; ++i) {
         result.put(transport.readArray(), transport.readArray());
      }
      return result;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }
}
