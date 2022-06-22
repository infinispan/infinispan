package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;

/**
 *
 */
public class GetAllParallelOperation<K, V> extends ParallelHotRodOperation<Map<K, V>, GetAllOperation<K, V>> {

   private final Set<byte[]> keys;

   protected GetAllParallelOperation(OperationContext operationContext, Set<byte[]> keys, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, options, dataFormat);
      this.keys = keys;
   }

   @Override
   protected List<GetAllOperation<K, V>> mapOperations() {
      Map<SocketAddress, Set<byte[]>> splittedKeys = new HashMap<>();

      for (byte[] key : keys) {
         SocketAddress socketAddress = operationContext.getChannelFactory().getHashAwareServer(key, operationContext.getCacheNameBytes());
         Set<byte[]> keys = splittedKeys.computeIfAbsent(socketAddress, k -> new HashSet<>());
         keys.add(key);
      }

      return splittedKeys.values().stream().map(
            keysSubset -> new GetAllOperation<K, V>(operationContext, keysSubset, options, dataFormat())).collect(Collectors.toList());
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
