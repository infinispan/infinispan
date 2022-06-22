package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;

/**
 *
 */
public class PutAllParallelOperation extends ParallelHotRodOperation<Void, PutAllOperation> {

   protected final Map<byte[], byte[]> map;

   public PutAllParallelOperation(OperationContext operationContext, Map<byte[], byte[]> map, CacheWriteOptions options,
                                  DataFormat dataFormat) {
      super(operationContext, options, dataFormat);
      this.map = map;
   }

   @Override
   protected List<PutAllOperation> mapOperations() {
      Map<SocketAddress, Map<byte[], byte[]>> splittedMaps = new HashMap<>();

      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
         SocketAddress socketAddress = operationContext.getChannelFactory().getHashAwareServer(entry.getKey(), operationContext.getCacheNameBytes());
         Map<byte[], byte[]> keyValueMap = splittedMaps.computeIfAbsent(socketAddress, k -> new HashMap<>());
         keyValueMap.put(entry.getKey(), entry.getValue());
      }

      return splittedMaps.values().stream().map(
            mapSubset -> new PutAllOperation(operationContext, mapSubset, (CacheWriteOptions) options, dataFormat())).collect(Collectors.toList());
   }

   @Override
   protected Void createCollector() {
      return null;
   }

   @Override
   protected void combine(Void collector, Void result) {
      // Nothing to do
   }

}
