package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

/**
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public class PutAllParallelOperation extends ParallelHotRodOperation<Void, PutAllOperation> {

   protected final Map<byte[], byte[]> map;
   protected final long lifespan;
   private final TimeUnit lifespanTimeUnit;
   protected final long maxIdle;
   private final TimeUnit maxIdleTimeUnit;

   public PutAllParallelOperation(Codec codec, ChannelFactory channelFactory, Map<byte[], byte[]> map, byte[]
         cacheName, AtomicInteger topologyId, int flags, Configuration cfg, long lifespan,
                                  TimeUnit lifespanTimeUnit, long maxIdle,
                                  TimeUnit maxIdleTimeUnit) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
      this.map = map;
      this.lifespan = lifespan;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdle = maxIdle;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   protected List<PutAllOperation> mapOperations() {
      Map<SocketAddress, Map<byte[], byte[]>> splittedMaps = new HashMap<>();

      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
         SocketAddress socketAddress = channelFactory.getSocketAddress(entry.getKey(), cacheName);
         Map<byte[], byte[]> keyValueMap = splittedMaps.get(socketAddress);
         if (keyValueMap == null) {
            keyValueMap = new HashMap<>();
            splittedMaps.put(socketAddress, keyValueMap);
         }
         keyValueMap.put(entry.getKey(), entry.getValue());
      }

      return splittedMaps.values().stream().map(
            mapSubset -> new PutAllOperation(codec, channelFactory, mapSubset, cacheName, topologyId, flags,
                  cfg, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit)).collect(Collectors.toList());
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
