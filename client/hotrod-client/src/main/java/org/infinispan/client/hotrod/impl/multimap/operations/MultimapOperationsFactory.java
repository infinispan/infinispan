package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import net.jcip.annotations.Immutable;

/**
 * Factory for {@link HotRodOperation} objects on Multimap.
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class MultimapOperationsFactory {

   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<>();

   private final ChannelFactory transportFactory;

   private final byte[] cacheNameBytes;

   private final AtomicInteger topologyId;

   private final boolean forceReturnValue;

   private final Codec codec;

   private final Configuration cfg;

   private final DataFormat dataFormat;

   private final ClientStatistics clientStatistics;

   public MultimapOperationsFactory(ChannelFactory channelFactory, String cacheName, Codec codec, Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      this.transportFactory = channelFactory;
      this.cacheNameBytes = cacheName == null ? null : RemoteCacheManager.cacheNameBytes(cacheName);
      this.topologyId = channelFactory != null
            ? channelFactory.createTopologyId(cacheNameBytes)
            : new AtomicInteger(-1);
      this.forceReturnValue = cfg.forceReturnValues();
      this.codec = codec;
      this.cfg = cfg;
      this.dataFormat = dataFormat;
      this.clientStatistics = clientStatistics;
   }

   public <K, V> GetKeyMultimapOperation<V> newGetKeyMultimapOperation(K key, byte[] keyBytes) {
      return new GetKeyMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat, clientStatistics);
   }

   public <K, V> GetKeyWithMetadataMultimapOperation<V> newGetKeyWithMetadataMultimapOperation(K key, byte[] keyBytes) {
      return new GetKeyWithMetadataMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat, clientStatistics);
   }

   public <K> PutKeyValueMultimapOperation newPutKeyValueOperation(K key, byte[] keyBytes, byte[] value,
                                                                   long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return new PutKeyValueMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, null, clientStatistics);
   }

   public <K> RemoveKeyMultimapOperation newRemoveKeyOperation(K key, byte[] keyBytes) {
      return new RemoveKeyMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, clientStatistics);
   }

   public <K> RemoveEntryMultimapOperation newRemoveEntryOperation(K key, byte[] keyBytes, byte[] value) {
      return new RemoveEntryMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, value, clientStatistics);
   }

   public <K> ContainsEntryMultimapOperation newContainsEntryOperation(K key, byte[] keyBytes, byte[] value) {
      return new ContainsEntryMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, value, clientStatistics);
   }

   public <K> ContainsKeyMultimapOperation newContainsKeyOperation(K key, byte[] keyBytes) {
      return new ContainsKeyMultimapOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, clientStatistics);
   }

   public ContainsValueMultimapOperation newContainsValueOperation(byte[] value) {
      return new ContainsValueMultimapOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS);
   }

   public SizeMultimapOperation newSizeOperation() {
      return new SizeMultimapOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public int flags() {
      Integer threadLocalFlags = this.flagsMap.get();
      this.flagsMap.remove();
      int intFlags = 0;
      if (threadLocalFlags != null) {
         intFlags |= threadLocalFlags.intValue();
      }
      if (forceReturnValue) {
         intFlags |= Flag.FORCE_RETURN_VALUE.getFlagInt();
      }
      return intFlags;
   }

   private int flags(long lifespan, long maxIdle) {
      int intFlags = flags();
      if (lifespan == 0) {
         intFlags |= Flag.DEFAULT_LIFESPAN.getFlagInt();
      }
      if (maxIdle == 0) {
         intFlags |= Flag.DEFAULT_MAXIDLE.getFlagInt();
      }
      return intFlags;
   }
}
