package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public class GetAllParallelOperation<K, V> extends ParallelHotRodOperation<Map<K, V>, GetAllOperation<K, V>> {

   private final Set<byte[]> keys;

   protected GetAllParallelOperation(Codec codec, TransportFactory transportFactory, Set<byte[]> keys, byte[]
         cacheName, AtomicInteger topologyId, int flags, Configuration cfg, ExecutorService executorService) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg, executorService);
      this.keys = keys;
   }

   @Override
   protected List<GetAllOperation<K, V>> mapOperations() {
      Map<SocketAddress, Set<byte[]>> splittedKeys = new HashMap<>();

      for (byte[] key : keys) {
         SocketAddress socketAddress = transportFactory.getSocketAddress(key, cacheName);
         Set<byte[]> keys = splittedKeys.get(socketAddress);
         if (keys == null) {
            keys = new HashSet<>();
            splittedKeys.put(socketAddress, keys);
         }
         keys.add(key);
      }

      return splittedKeys.values().stream().map(
            keysSubset -> new GetAllOperation<K, V>(codec, transportFactory, keysSubset, cacheName, topologyId,
                  flags, cfg)).collect(Collectors.toList());
   }

   @Override
   protected Map<K, V> createCollector() {
      return new HashMap<>();
   }

   @Override
   protected void combine(Map<K, V> collector, Map<K, V> result) {
      collector.putAll(result);
   }
}
