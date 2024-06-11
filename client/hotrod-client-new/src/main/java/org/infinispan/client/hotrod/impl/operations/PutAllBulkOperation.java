package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;

public class PutAllBulkOperation extends HotRodBulkOperation<Map<byte[], byte[]>, Void, HotRodOperation<Void>> {
   private final Map<?, ?> values;

   public PutAllBulkOperation(Map<?, ?> values, DataFormat dataFormat,
                              Function<Map<byte[], byte[]>, HotRodOperation<Void>> setPutAllOperationFunction) {
      super(dataFormat, setPutAllOperationFunction);
      this.values = values;
   }

   @Override
   protected Map<SocketAddress, HotRodOperation<Void>> gatherOperations(
         Function<Object, SocketAddress> routingFunction) {
      Map<SocketAddress, Map<byte[], byte[]>> serializedKeys = new HashMap<>();
      for (Map.Entry<?, ?> entry : values.entrySet()) {
         Object key = entry.getKey();
         byte[] keyBytes = dataFormat.keyToBytes(key);
         SocketAddress socketAddress = getAddressForKey(key, keyBytes, routingFunction);
         Map<byte[], byte[]> mapBytes = serializedKeys.computeIfAbsent(socketAddress, ___ -> new HashMap<>());
         mapBytes.put(keyBytes, dataFormat.valueToBytes(entry.getValue()));
      }
      return serializedKeys.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e ->
               opFunction.apply(e.getValue())));
   }

   @Override
   public CompletionStage<Void> reduce(CompletionStage<Collection<Void>> stage) {
      return stage
            .thenApply(___ -> null);
   }
}
