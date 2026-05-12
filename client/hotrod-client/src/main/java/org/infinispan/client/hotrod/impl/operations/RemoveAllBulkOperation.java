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

public class RemoveAllBulkOperation extends HotRodBulkOperation<Set<byte[]>, Void, HotRodOperation<Void>> {
   private final Set<?> keys;

   public RemoveAllBulkOperation(Set<?> keys, DataFormat dataFormat,
                                 Function<Set<byte[]>, HotRodOperation<Void>> opFunction) {
      super(dataFormat, opFunction);
      this.keys = keys;
   }

   @Override
   protected Map<SocketAddress, HotRodOperation<Void>> gatherOperations(
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
   public CompletionStage<Void> reduce(CompletionStage<Collection<Void>> stage) {
      return stage.thenApply(___ -> null);
   }
}
