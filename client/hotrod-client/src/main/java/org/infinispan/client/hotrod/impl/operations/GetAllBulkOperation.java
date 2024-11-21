package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;

public class GetAllBulkOperation<K, V> extends HotRodBulkOperation<Set<byte[]>, Map<K, V>, HotRodOperation<Map<K, V>>> {
   private final Set<?> keys;

   public GetAllBulkOperation(Set<?> keys, DataFormat dataFormat,
                              Function<Set<byte[]>, HotRodOperation<Map<K, V>>> setGetAllOperationFunction) {
      super(dataFormat, setGetAllOperationFunction);
      this.keys = keys;
   }

   @Override
   protected Map<SocketAddress, HotRodOperation<Map<K, V>>> gatherOperations(
         Function<Object, SocketAddress> routingFunction) {
      Map<SocketAddress, Set<byte[]>> serializedKeys = new HashMap<>();
      for (Object key : keys) {
         byte[] bytes = dataFormat.keyToBytes(key);
         SocketAddress socketAddress = getAddressForKey(key, bytes, routingFunction);
         Set<byte[]> keyBytes = serializedKeys.computeIfAbsent(socketAddress, ___ -> new HashSet<>());
         keyBytes.add(bytes);
      }
      return serializedKeys.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e ->
               opFunction.apply(e.getValue())));
   }

   @Override
   public CompletionStage<Map<K, V>> reduce(CompletionStage<Collection<Map<K, V>>> stage) {
      return stage
            .thenApply(c -> c.stream()
                  .reduce((m1, m2) -> {
                     m1.putAll(m2);
                     return m1;
                  }).orElse(Map.of()));
   }
}
