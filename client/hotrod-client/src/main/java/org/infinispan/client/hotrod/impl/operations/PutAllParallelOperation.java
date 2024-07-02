package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public class PutAllParallelOperation extends ParallelHotRodOperation<Void, PutAllOperation> {

   protected final Map<byte[], byte[]> map;
   protected final long lifespan;
   private final TimeUnit lifespanTimeUnit;
   protected final long maxIdle;
   private final TimeUnit maxIdleTimeUnit;
   private final TelemetryService telemetryService;

   public PutAllParallelOperation(Codec codec, ChannelFactory channelFactory, Map<byte[], byte[]> map, byte[]
         cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, long lifespan,
                                  TimeUnit lifespanTimeUnit, long maxIdle,
                                  TimeUnit maxIdleTimeUnit, DataFormat dataFormat, ClientStatistics clientStatistics,
                                  TelemetryService telemetryService) {
      super(codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat, clientStatistics);
      this.map = map;
      this.lifespan = lifespan;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdle = maxIdle;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      this.telemetryService = telemetryService;
   }

   @Override
   protected List<PutAllOperation> mapOperations() {
      Map<SocketAddress, Map<byte[], byte[]>> splittedMaps = new HashMap<>();

      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
         SocketAddress socketAddress = channelFactory.getHashAwareServer(entry.getKey(), cacheName());
         Map<byte[], byte[]> keyValueMap = splittedMaps.computeIfAbsent(socketAddress, k -> new HashMap<>());
         keyValueMap.put(entry.getKey(), entry.getValue());
      }

      return splittedMaps.values().stream().map(
            mapSubset -> new PutAllOperation(codec, channelFactory, mapSubset, cacheName(), header.getClientTopology(), flags(),
                  cfg, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat(), clientStatistics,
                  telemetryService)).collect(Collectors.toList());
   }

   @Override
   protected Void createCollector() {
      return null;
   }

   @Override
   protected void combine(Void collector, Void result) {
      // Nothing to do
   }


   @Override
   public void writeBytes(Channel channel, ByteBuf buffer) {
      throw new UnsupportedOperationException("TODO!");
   }
}
