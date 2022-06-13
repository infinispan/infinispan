package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.commons.util.IntSet;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.CacheTopologyInfo;
import org.infinispan.hotrod.impl.cache.ClientStatistics;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hotrod.impl.iteration.KeyTracker;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.query.RemoteQuery;
import org.infinispan.hotrod.impl.transaction.entry.Modification;
import org.infinispan.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.channel.Channel;

/**
 * Factory for {@link org.infinispan.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @since 14.0
 */
public class CacheOperationsFactory implements HotRodConstants {
   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<>();
   private final OperationContext cacheContext;
   private final OperationContext defaultContext;

   public CacheOperationsFactory(ChannelFactory channelFactory, String cacheName, Codec codec, ClientListenerNotifier listenerNotifier, HotRodConfiguration configuration, ClientStatistics clientStatistics) {
      this.cacheContext = new OperationContext(channelFactory, codec, listenerNotifier, configuration, clientStatistics, cacheName);
      this.defaultContext = new OperationContext(channelFactory, codec, listenerNotifier, configuration, clientStatistics, null);
   }

   public CacheOperationsFactory(ChannelFactory channelFactory, Codec codec, ClientListenerNotifier listenerNotifier, HotRodConfiguration configuration) {
      this(channelFactory, null, codec, listenerNotifier, configuration, null);
   }

   public OperationContext getDefaultContext() {
      return defaultContext;
   }

   public OperationContext getCacheContext() {
      return cacheContext;
   }

   public Codec getCodec() {
      return cacheContext.getCodec();
   }

   public void setCodec(Codec codec) {
      cacheContext.setCodec(codec);
      defaultContext.setCodec(codec);
   }

   public <K, V> GetOperation<K, V> newGetKeyOperation(K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      return new GetOperation<>(cacheContext, key, keyBytes, options, dataFormat);
   }

   public <K, V> GetAllParallelOperation<K, V> newGetAllOperation(Set<byte[]> keys, CacheOptions options, DataFormat dataFormat) {
      return new GetAllParallelOperation<>(cacheContext, keys, options, dataFormat);
   }

   public <K, V> GetAndRemoveOperation<K, V> newGetAndRemoveOperation(K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      return new GetAndRemoveOperation<>(cacheContext, key, keyBytes, options, dataFormat);
   }

   public <K> RemoveOperation<K> newRemoveOperation(K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      return new RemoveOperation<>(cacheContext, key, keyBytes, options, dataFormat);
   }

   public <K, V> RemoveIfUnmodifiedOperation<K, V> newRemoveIfUnmodifiedOperation(K key, byte[] keyBytes, long version, CacheOptions options, DataFormat dataFormat) {
      return new RemoveIfUnmodifiedOperation<>(cacheContext, key, keyBytes, version, options, dataFormat);
   }

   public <K, V> ReplaceIfUnmodifiedOperation<K, V> newReplaceIfUnmodifiedOperation(K key, byte[] keyBytes, byte[] value, long version, CacheWriteOptions options, DataFormat dataFormat) {
      return new ReplaceIfUnmodifiedOperation<>(cacheContext, key, keyBytes, value, version, options, dataFormat);
   }

   public <K, V> GetWithMetadataOperation<K, V> newGetWithMetadataOperation(K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      return newGetWithMetadataOperation(key, keyBytes, options, dataFormat, null);
   }

   public <K, V> GetWithMetadataOperation<K, V> newGetWithMetadataOperation(K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat, SocketAddress listenerServer) {
      return new GetWithMetadataOperation<>(cacheContext, key, keyBytes, options, dataFormat, listenerServer);
   }

   public StatsOperation newStatsOperation(CacheOptions options) {
      return new StatsOperation(cacheContext, options);
   }

   public <K, V> PutOperation<K, V> newPutKeyValueOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      return new PutOperation<>(cacheContext, key, keyBytes, value, options, dataFormat);
   }

   public <K> SetOperation<K> newSetKeyValueOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      return new SetOperation<>(cacheContext, key, keyBytes, value, options, dataFormat);
   }

   public PutAllParallelOperation newPutAllOperation(Map<byte[], byte[]> map, CacheWriteOptions options, DataFormat dataFormat) {
      return new PutAllParallelOperation(cacheContext, map, options, dataFormat);
   }

   public <K, V> PutIfAbsentOperation<K, V> newPutIfAbsentOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      return new PutIfAbsentOperation<>(cacheContext, key, keyBytes, value, options, dataFormat);
   }

   public <K> SetIfAbsentOperation<K> newSetIfAbsentOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      return new SetIfAbsentOperation<>(cacheContext, key, keyBytes, value, options, dataFormat);
   }

   public <K, V> ReplaceOperation<K, V> newReplaceOperation(K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      return new ReplaceOperation<>(cacheContext, key, keyBytes, value, options, dataFormat);
   }

   public ContainsKeyOperation newContainsKeyOperation(Object key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      return new ContainsKeyOperation(cacheContext, key, keyBytes, options, dataFormat);
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(cacheContext, CacheOptions.DEFAULT);
   }

   public <K> BulkGetKeysOperation<K> newBulkGetKeysOperation(int scope, CacheOptions options, DataFormat dataFormat) {
      return new BulkGetKeysOperation<>(cacheContext, options, scope, dataFormat);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener, CacheOptions options, DataFormat dataFormat) {
      return new AddClientListenerOperation(cacheContext, options, listener, null, null, dataFormat, null);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener, byte[][] filterFactoryParams, byte[][] converterFactoryParams, CacheOptions options, DataFormat dataFormat) {
      return new AddClientListenerOperation(cacheContext, options, listener, filterFactoryParams, converterFactoryParams, dataFormat, null);
   }

   public RemoveClientListenerOperation newRemoveClientListenerOperation(Object listener, CacheOptions options) {
      return new RemoveClientListenerOperation(cacheContext, options, listener);
   }

   public AddBloomNearCacheClientListenerOperation newAddNearCacheListenerOperation(Object listener, CacheOptions options, DataFormat dataFormat, int bloomFilterBits, RemoteCache<?, ?> remoteCache) {
      return new AddBloomNearCacheClientListenerOperation(cacheContext, options, listener, dataFormat, bloomFilterBits, remoteCache);
   }

   public UpdateBloomFilterOperation newUpdateBloomFilterOperation(CacheOptions options, SocketAddress address, byte[] bloomBytes) {
      return new UpdateBloomFilterOperation(cacheContext, options, address, bloomBytes);
   }

   /**
    * Construct a ping request directed to a particular node.
    *
    * @param releaseChannel
    * @return a ping operation for a particular node
    */
   public PingOperation newPingOperation(boolean releaseChannel) {
      return new PingOperation(cacheContext, releaseChannel);
   }

   /**
    * Construct a fault tolerant ping request. This operation should be capable to deal with nodes being down, so it
    * will find the first node successful node to respond to the ping.
    *
    * @return a ping operation for the cluster
    */
   public FaultTolerantPingOperation newFaultTolerantPingOperation() {
      return new FaultTolerantPingOperation(cacheContext, CacheOptions.DEFAULT);
   }

   public QueryOperation newQueryOperation(RemoteQuery remoteQuery, CacheOptions options, DataFormat dataFormat) {
      return new QueryOperation(cacheContext, options, remoteQuery, dataFormat);
   }

   public SizeOperation newSizeOperation(CacheOptions options) {
      return new SizeOperation(cacheContext, options);
   }

   public <T> ExecuteOperation<T> newExecuteOperation(String taskName, Map<String, byte[]> marshalledParams, Object key, CacheOptions options, DataFormat dataFormat) {
      return new ExecuteOperation<>(cacheContext, options, taskName, marshalledParams, key, dataFormat);
   }

   public AdminOperation newAdminOperation(String taskName, Map<String, byte[]> marshalledParams, CacheOptions options) {
      return new AdminOperation(cacheContext, options, taskName, marshalledParams);
   }

   public CacheTopologyInfo getCacheTopologyInfo() {
      return cacheContext.getChannelFactory().getCacheTopologyInfo(cacheContext.getCacheNameBytes());
   }

   /**
    * Returns a map containing for each address all of its primarily owned segments. If the primary segments are not
    * known an empty map will be returned instead
    *
    * @return map containing addresses and their primary segments
    */
   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByAddress() {
      return cacheContext.getChannelFactory().getPrimarySegmentsByAddress(cacheContext.getCacheNameBytes());
   }

   public ConsistentHash getConsistentHash() {
      return cacheContext.getChannelFactory().getConsistentHash(cacheContext.getCacheNameBytes());
   }

   public int getTopologyId() {
      return cacheContext.getTopologyId().get();
   }

   public IterationStartOperation newIterationStartOperation(String filterConverterFactory, byte[][] filterParameters, IntSet segments, int batchSize, boolean metadata, CacheOptions options, DataFormat dataFormat, SocketAddress targetAddress) {
      return new IterationStartOperation(cacheContext, options, filterConverterFactory, filterParameters, segments, batchSize, metadata, dataFormat, targetAddress);
   }

   public IterationEndOperation newIterationEndOperation(byte[] iterationId, CacheOptions options, Channel channel) {
      return new IterationEndOperation(cacheContext, options, iterationId, channel);
   }

   public <K, E> IterationNextOperation<K, E> newIterationNextOperation(byte[] iterationId, Channel channel, KeyTracker segmentKeyTracker, CacheOptions options, DataFormat dataFormat) {
      return new IterationNextOperation<>(cacheContext, options, iterationId, channel, segmentKeyTracker, dataFormat);
   }

   public <K> GetStreamOperation<K> newGetStreamOperation(K key, byte[] keyBytes, int offset, CacheOptions options) {
      return new GetStreamOperation<>(cacheContext, key, keyBytes, offset, options);
   }

   public <K> PutStreamOperation<K> newPutStreamOperation(K key, byte[] keyBytes, long version, CacheWriteOptions options) {
      return new PutStreamOperation<>(cacheContext, key, keyBytes, options, version);
   }

   public <K> PutStreamOperation<K> newPutStreamOperation(K key, byte[] keyBytes, CacheWriteOptions options) {
      return new PutStreamOperation<>(cacheContext, key, keyBytes, options, PutStreamOperation.VERSION_PUT);
   }

   public <K> PutStreamOperation<K> newPutIfAbsentStreamOperation(K key, byte[] keyBytes, CacheWriteOptions options) {
      return new PutStreamOperation<>(cacheContext, key, keyBytes, options, PutStreamOperation.VERSION_PUT_IF_ABSENT);
   }

   public AuthMechListOperation newAuthMechListOperation(Channel channel) {
      return new AuthMechListOperation(cacheContext, channel);
   }

   public AuthOperation newAuthOperation(Channel channel, String saslMechanism, byte[] response) {
      return new AuthOperation(cacheContext, channel, saslMechanism, response);
   }

   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit, List<Modification> modifications, boolean recoverable, long timeoutMs) {
      return new PrepareTransactionOperation(cacheContext, xid, onePhaseCommit, modifications, recoverable, timeoutMs);
   }
}
