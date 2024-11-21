package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

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

   private final AtomicReference<ClientTopology> clientTopologyRef;

   private final boolean forceReturnValue;

   private final Configuration cfg;

   private final DataFormat dataFormat;

   private final ClientStatistics clientStatistics;

   public MultimapOperationsFactory(ChannelFactory channelFactory, String cacheName, Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      this.transportFactory = channelFactory;
      this.cacheNameBytes = cacheName == null ? null : RemoteCacheManager.cacheNameBytes(cacheName);
      this.clientTopologyRef = channelFactory != null
            ? channelFactory.createTopologyId(cacheNameBytes)
            : new AtomicReference<>(new ClientTopology(-1, cfg.clientIntelligence()));
      this.forceReturnValue = cfg.forceReturnValues();
      this.cfg = cfg;
      this.dataFormat = dataFormat;
      this.clientStatistics = clientStatistics;
   }

   public <K, V> GetKeyMultimapOperation<V> newGetKeyMultimapOperation(K key, byte[] keyBytes, boolean supportsDuplicates) {
      return new GetKeyMultimapOperation<>(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, dataFormat, clientStatistics, supportsDuplicates);
   }

   public <K, V> GetKeyWithMetadataMultimapOperation<V> newGetKeyWithMetadataMultimapOperation(K key, byte[] keyBytes, boolean supportsDuplicates) {
      return new GetKeyWithMetadataMultimapOperation<>(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, dataFormat, clientStatistics, supportsDuplicates);
   }

   public <K> PutKeyValueMultimapOperation newPutKeyValueOperation(K key, byte[] keyBytes, byte[] value,
                                                                   long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      return new PutKeyValueMultimapOperation(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, null, clientStatistics, supportsDuplicates);
   }

   public <K> RemoveKeyMultimapOperation newRemoveKeyOperation(K key, byte[] keyBytes, boolean supportsDuplicates) {
      return new RemoveKeyMultimapOperation(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, clientStatistics, supportsDuplicates);
   }

   public <K> RemoveEntryMultimapOperation newRemoveEntryOperation(K key, byte[] keyBytes, byte[] value, boolean supportsDuplicates) {
      return new RemoveEntryMultimapOperation(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, value, clientStatistics, supportsDuplicates);
   }

   public <K> ContainsEntryMultimapOperation newContainsEntryOperation(K key, byte[] keyBytes, byte[] value, boolean supportsDuplicates) {
      return new ContainsEntryMultimapOperation(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, value, clientStatistics, supportsDuplicates);
   }

   public <K> ContainsKeyMultimapOperation newContainsKeyOperation(K key, byte[] keyBytes, boolean supportsDuplicates) {
      return new ContainsKeyMultimapOperation(
            transportFactory, key, keyBytes, cacheNameBytes, clientTopologyRef, flags(), cfg, clientStatistics, supportsDuplicates);
   }

   public ContainsValueMultimapOperation newContainsValueOperation(byte[] value, boolean supportsDuplicates) {
      return new ContainsValueMultimapOperation(
            transportFactory.getNegotiatedCodec(), transportFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, supportsDuplicates);
   }

   public SizeMultimapOperation newSizeOperation(boolean supportsDuplicates) {
      return new SizeMultimapOperation(
            transportFactory.getNegotiatedCodec(), transportFactory, cacheNameBytes, clientTopologyRef, flags(), cfg, supportsDuplicates);
   }

   public int flags() {
      Integer threadLocalFlags = this.flagsMap.get();
      this.flagsMap.remove();
      int intFlags = 0;
      if (threadLocalFlags != null) {
         intFlags |= threadLocalFlags;
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
