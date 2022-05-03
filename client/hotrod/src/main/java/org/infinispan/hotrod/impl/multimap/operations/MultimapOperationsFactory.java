package org.infinispan.hotrod.impl.multimap.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.HotRodFlag;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.ClientStatistics;
import org.infinispan.hotrod.impl.operations.HotRodOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

import net.jcip.annotations.Immutable;

/**
 * Factory for {@link HotRodOperation} objects on Multimap.
 *
 * @since 14.0
 */
@Immutable
public class MultimapOperationsFactory {
   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<>();
   private final OperationContext operationContext;
   private final DataFormat dataFormat;

   public MultimapOperationsFactory(ChannelFactory channelFactory, String cacheName, HotRodConfiguration configuration, Codec codec, DataFormat dataFormat, ClientStatistics clientStatistics) {
      this.operationContext = new OperationContext(channelFactory, codec, null, configuration, clientStatistics, cacheName);
      this.dataFormat = dataFormat;
   }

   public <K, V> GetKeyMultimapOperation<K, V> newGetKeyMultimapOperation(K key, byte[] keyBytes, CacheOptions options) {
      return new GetKeyMultimapOperation<>(operationContext, key, keyBytes, options, dataFormat);
   }

   public <K, V> GetKeyWithMetadataMultimapOperation<K, V> newGetKeyWithMetadataMultimapOperation(K key, byte[] keyBytes, CacheOptions options) {
      return new GetKeyWithMetadataMultimapOperation<>(operationContext, key, keyBytes, options, dataFormat);
   }

   public <K> PutKeyValueMultimapOperation<K> newPutKeyValueOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options) {
      return new PutKeyValueMultimapOperation<>(operationContext, key, keyBytes, value, options, null);
   }

   public <K> RemoveKeyMultimapOperation<K> newRemoveKeyOperation(K key, byte[] keyBytes, CacheOptions options) {
      return new RemoveKeyMultimapOperation<>(operationContext, key, keyBytes, options);
   }

   public <K> RemoveEntryMultimapOperation<K> newRemoveEntryOperation(K key, byte[] keyBytes, byte[] value, CacheOptions options) {
      return new RemoveEntryMultimapOperation<>(operationContext, key, keyBytes, value, options);
   }

   public <K> ContainsEntryMultimapOperation<K> newContainsEntryOperation(K key, byte[] keyBytes, byte[] value, CacheOptions options) {
      return new ContainsEntryMultimapOperation<>(operationContext, key, keyBytes, value, options);
   }

   public <K> ContainsKeyMultimapOperation<K> newContainsKeyOperation(K key, byte[] keyBytes, CacheOptions options) {
      return new ContainsKeyMultimapOperation<>(operationContext, key, keyBytes, options);
   }

   public ContainsValueMultimapOperation newContainsValueOperation(byte[] value, CacheOptions options) {
      return new ContainsValueMultimapOperation(operationContext, flags(), value, options);
   }

   public SizeMultimapOperation newSizeOperation() {
      return new SizeMultimapOperation(operationContext, CacheOptions.DEFAULT);
   }

   public int flags() {
      Integer threadLocalFlags = this.flagsMap.get();
      this.flagsMap.remove();
      int intFlags = 0;
      if (threadLocalFlags != null) {
         intFlags |= threadLocalFlags.intValue();
      }
      return intFlags;
   }

   private int flags(long lifespan, long maxIdle) {
      int intFlags = flags();
      if (lifespan == 0) {
         intFlags |= HotRodFlag.DEFAULT_LIFESPAN.getFlagInt();
      }
      if (maxIdle == 0) {
         intFlags |= HotRodFlag.DEFAULT_MAXIDLE.getFlagInt();
      }
      return intFlags;
   }
}
