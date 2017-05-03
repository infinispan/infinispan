package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
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
public class GetAllOperation<K, V> extends RetryOnFailureOperation<Map<K, V>> {

   public GetAllOperation(Codec codec, TransportFactory transportFactory,
                          Set<byte[]> keys, byte[] cacheName, AtomicInteger topologyId,
                          int flags, Configuration cfg) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
      this.keys = keys;
   }

   protected final Set<byte[]> keys;

   @Override
   protected Map<K, V> executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, GET_ALL_REQUEST);
      transport.writeVInt(keys.size());
      for (byte[] key : keys) {
         transport.writeArray(key);
      }
      transport.flush();

      short status = readHeaderAndValidate(transport, params);
      int size = transport.readVInt();
      Map<K, V> result = new HashMap<K, V>(size);
      for (int i = 0; i < size; ++i) {
         K key = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
         V value = codec.readUnmarshallByteArray(transport, status, cfg.serialWhitelist());
         result.put(key, value);
      }
      return result;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(keys.iterator().next(), failedServers, cacheName);
   }
}
