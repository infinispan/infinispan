package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public class GetAllParallelOperation<K, V> extends ParallelHotRodOperation<Map<K, V>, GetAllOperation<K, V>> {

   private final Set<byte[]> keys;

   protected GetAllParallelOperation(Codec codec, ChannelFactory channelFactory, Set<byte[]> keys, byte[]
         cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat, clientStatistics);
      this.keys = keys;
   }

   @Override
   protected List<GetAllOperation<K, V>> mapOperations() {
      Map<SocketAddress, Set<byte[]>> splittedKeys = new HashMap<>();

      for (byte[] key : keys) {
         SocketAddress socketAddress = channelFactory.getHashAwareServer(key, cacheName());
         Set<byte[]> keys = splittedKeys.computeIfAbsent(socketAddress, k -> new HashSet<>());
         keys.add(key);
      }

      return splittedKeys.values().stream().map(
            keysSubset -> new GetAllOperation<K, V>(codec, channelFactory, keysSubset, cacheName(), header.getClientTopology(),
                  flags(), cfg, dataFormat(), clientStatistics)).collect(Collectors.toList());
   }

   @Override
   protected Map<K, V> createCollector() {
      return new HashMap<>();
   }

   @Override
   protected void combine(Map<K, V> collector, Map<K, V> result) {
      collector.putAll(result);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buffer) {
      throw new UnsupportedOperationException("TODO!");
   }
}
