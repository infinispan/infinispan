package org.infinispan.client.hotrod.impl.operations;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transaction.operations.PrepareTransactionOperation;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public interface CacheOperationsFactory {
   InternalRemoteCache<?, ?> getRemoteCache();
   <V> HotRodOperation<V> newGetOperation(Object key);

   HotRodOperation<PingResponse> newPingOperation();

   <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key);

   PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                              List<Modification> modifications,
                                                              boolean recoverable, long timeoutMs);

   HotRodOperation<Void> newRemoveClientListenerOperation(Object listener);

   HotRodOperation<IterationStartResponse> newIterationStartOperation(String filterConverterFactory, byte[][] filterParams,
                                                                      IntSet segments, int batchSize, boolean metadata);

   <K, E> HotRodOperation<IterationNextResponse<K, E>> newIterationNextOperation(byte[] iterationId, KeyTracker segmentKeyTracker);

   HotRodOperation<IterationEndResponse> newIterationEndOperation(byte[] iterationId);

   HotRodOperation<Void> newClearOperation();

   <K, V> HotRodOperation<MetadataValue<V>> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <V> HotRodOperation<MetadataValue<V>> newRemoveOperation(Object key);

   <K> HotRodOperation<Boolean> newContainsKeyOperation(K key);

   <K, V> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit,
                                                 long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                                    long maxIdleTime, TimeUnit maxIdleTimeUnit);

   <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit,
                                                                    long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags);

   HotRodOperation<ServerStatistics> newStatsOperation();

   HotRodOperation<Integer> newSizeOperation();

   /**
    * This method should not be invoked by callers normally as it bypasses other factory checks.
    * Please use {@link PutAllBulkOperation(Set)} instead, passing this method as the Function
    * @param map
    * @param lifespan
    * @param lifespanUnit
    * @param maxIdleTime
    * @param maxIdleTimeUnit
    * @return
    */
   HotRodOperation<Void> newPutAllBytesOperation(Map<byte[], byte[]> map, long lifespan, TimeUnit lifespanUnit,
                                           long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * This method should not be invoked by callers normally as it bypasses other factory checks.
    * Please use {@link GetAllBulkOperation(Set)} instead, passing this method as the Function
    * @param keys
    * @return
    * @param <K>
    * @param <V>
    */
   <K, V> HotRodOperation<Map<K, V>> newGetAllBytesOperation(Set<byte[]> keys);

   <K, V> HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> newGetWithMetadataOperation(K key, Channel channel);

   <K, V> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan,
                                                                                         TimeUnit lifespanTimeUnit, long maxIdle,
                                                                                         TimeUnit maxIdleTimeUnit, long version);

   <K, V> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version);

   HotRodOperation<Void> newUpdateBloomFilterOperation(byte[] bloomFilterBits);

   ClientListenerOperation newAddNearCacheListenerOperation(Object listener, int bloomBits);

   <T> QueryOperation<T> newQueryOperation(RemoteQuery<T> ts, boolean withHitCount);

   AddClientListenerOperation newAddClientListenerOperation(Object listener);

   AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams,
                                                            Object[] converterFactoryParams);

   HotRodOperation<GetStreamStartResponse> newGetStreamStartOperation(Object key, int batchSize);

   HotRodOperation<GetStreamNextResponse> newGetStreamNextOperation(int id, Channel channel);

   GetStreamEndOperation newGetStreamEndOperation(int id);

   HotRodOperation<PutStreamResponse> newPutStreamStartOperation(Object key, long version, long lifespan,
                                                                 TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * Creates a new operation to be used for a stream write operation for a new chunk
    * @param id the id when first starting the put operationws
    * @param lastChunk whether this is the last chunk for the value
    * @param valueBytes the bytes to be appended, the operation will release when written
    * @param channel the channel this was written from
    * @return the operation to submit
    */
   HotRodOperation<Boolean> newPutStreamNextOperation(int id, boolean lastChunk, ByteBuf valueBytes, Channel channel);

   PutStreamEndOperation newPutStreamEndOperation(int id);

   byte[][] marshallParams(Object[] params);

   CacheOperationsFactory newFactoryFor(InternalRemoteCache<?, ?> internalRemoteCache);
}
