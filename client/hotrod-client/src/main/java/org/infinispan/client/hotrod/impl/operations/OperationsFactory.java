package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class OperationsFactory implements HotRodConstants {

   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<Integer>();

   private final TransportFactory transportFactory;

   private final byte[] cacheNameBytes;

   private final AtomicInteger topologyId;

   private final boolean forceReturnValue;

   private final Codec codec;

   private final ClientListenerNotifier listenerNotifier;

   private final String cacheName;

   private final ExecutorService executorService;

   private final Configuration cfg;

   public OperationsFactory(TransportFactory transportFactory, String cacheName, boolean forceReturnValue, Codec
           codec, ClientListenerNotifier listenerNotifier, ExecutorService executorService, Configuration cfg) {
      this.transportFactory = transportFactory;
      this.executorService = executorService;
      this.cacheNameBytes = RemoteCacheManager.cacheNameBytes(cacheName);
      this.cacheName = cacheName;
      this.topologyId = transportFactory != null
         ? transportFactory.createTopologyId(cacheNameBytes)
         : new AtomicInteger(-1);
      this.forceReturnValue = forceReturnValue;
      this.codec = codec;
      this.listenerNotifier = listenerNotifier;
      this.cfg = cfg;
   }

   public OperationsFactory(TransportFactory transportFactory, Codec codec, ExecutorService executorService, Configuration cfg) {
      this(transportFactory, null, false, codec, null, executorService, cfg);
   }

   public ClientListenerNotifier getListenerNotifier() {
      return listenerNotifier;
   }

   public byte[] getCacheName() {
      return cacheNameBytes;
   }

   public <V> GetOperation<V> newGetKeyOperation(Object key, byte[] keyBytes) {
      return new GetOperation<>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <K, V> GetAllParallelOperation<K, V> newGetAllOperation(Set<byte[]> keys) {
      return new GetAllParallelOperation<>(codec, transportFactory, keys, cacheNameBytes, topologyId, flags(),
            cfg, executorService);
   }

   public <V> RemoveOperation<V> newRemoveOperation(Object key, byte[] keyBytes) {
      return new RemoveOperation<>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <V> RemoveIfUnmodifiedOperation<V> newRemoveIfUnmodifiedOperation(Object key, byte[] keyBytes, long version) {
      return new RemoveIfUnmodifiedOperation<>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, version);
   }

   public ReplaceIfUnmodifiedOperation newReplaceIfUnmodifiedOperation(Object key, byte[] keyBytes,
            byte[] value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return new ReplaceIfUnmodifiedOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
   }

   public <V> GetWithVersionOperation<V> newGetWithVersionOperation(Object key, byte[] keyBytes) {
      return new GetWithVersionOperation<>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <V> GetWithMetadataOperation<V> newGetWithMetadataOperation(Object key, byte[] keyBytes) {
      return new GetWithMetadataOperation<>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg);
   }

   public StatsOperation newStatsOperation() {
      return new StatsOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <V> PutOperation<V> newPutKeyValueOperation(Object key, byte[] keyBytes, byte[] value,
          long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return new PutOperation<V>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   public PutAllParallelOperation newPutAllOperation(Map<byte[], byte[]> map,
                                                     long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return new PutAllParallelOperation(
            codec, transportFactory, map, cacheNameBytes, topologyId, flags(lifespan, maxIdle), cfg,
              lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, executorService);
   }

   public <V> PutIfAbsentOperation<V> newPutIfAbsentOperation(Object key, byte[] keyBytes, byte[] value,
             long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new PutIfAbsentOperation<V>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdleTime),
            cfg, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public <V> ReplaceOperation<V> newReplaceOperation(Object key, byte[] keyBytes, byte[] values,
           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return new ReplaceOperation<V>(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, values, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   public ContainsKeyOperation newContainsKeyOperation(Object key, byte[] keyBytes) {
      return new ContainsKeyOperation(
            codec, transportFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg);
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <K, V> BulkGetOperation<K, V> newBulkGetOperation(int size) {
      return new BulkGetOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg, size);
   }

   public <K> BulkGetKeysOperation<K> newBulkGetKeysOperation(int scope) {
      return new BulkGetKeysOperation<>(
         codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg, scope);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      return new AddClientListenerOperation(codec, transportFactory,
            cacheName, topologyId, flags(), cfg, listenerNotifier,
            listener, null, null);
   }

   public AddClientListenerOperation newAddClientListenerOperation(
         Object listener, byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      return new AddClientListenerOperation(codec, transportFactory,
            cacheName, topologyId, flags(), cfg, listenerNotifier,
            listener, filterFactoryParams, converterFactoryParams);
   }

   public RemoveClientListenerOperation newRemoveClientListenerOperation(Object listener) {
      return new RemoveClientListenerOperation(codec, transportFactory,
            cacheNameBytes, topologyId, flags(), cfg, listenerNotifier, listener);
   }

   /**
    * Construct a ping request directed to a particular node.
    *
    * @param transport represents the node to which the operation is directed
    * @return a ping operation for a particular node
    */
   public PingOperation newPingOperation(Transport transport) {
      return new PingOperation(codec, topologyId, cfg, transport, cacheNameBytes);
   }

   /**
    * Construct a fault tolerant ping request. This operation should be capable
    * to deal with nodes being down, so it will find the first node successful
    * node to respond to the ping.
    *
    * @return a ping operation for the cluster
    */
   public FaultTolerantPingOperation newFaultTolerantPingOperation() {
      return new FaultTolerantPingOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public QueryOperation newQueryOperation(RemoteQuery remoteQuery) {
      return new QueryOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg, remoteQuery);
   }

   public SizeOperation newSizeOperation() {
      return new SizeOperation(codec, transportFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <T> ExecuteOperation<T> newExecuteOperation(String taskName, Map<String, byte[]> marshalledParams) {
      return new ExecuteOperation<>(codec, transportFactory, cacheNameBytes,
            topologyId, flags(), cfg, taskName, marshalledParams);
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

   public void setFlags(Flag[] flags) {
      int intFlags = 0;
      for(Flag flag : flags)
         intFlags |= flag.getFlagInt();
      this.flagsMap.set(intFlags);
   }

   public void setFlags(int intFlags) {
      this.flagsMap.set(intFlags);
   }

   public void addFlag(Flag flag) {
      int intFlags = flag.getFlagInt();
      Integer threadLocalFlags = this.flagsMap.get();
      if (threadLocalFlags != null) {
         intFlags |= threadLocalFlags;
      }
      this.flagsMap.set(intFlags);
   }

   public boolean hasFlag(Flag flag) {
      Integer threadLocalFlags = this.flagsMap.get();
      return threadLocalFlags != null && (threadLocalFlags & flag.getFlagInt()) != 0;
   }

   public CacheTopologyInfo getCacheTopologyInfo() {
      return transportFactory.getCacheTopologyInfo(cacheNameBytes);
   }

   public IterationStartOperation newIterationStartOperation(String filterConverterFactory, byte[][] filterParameters, Set<Integer> segments, int batchSize, boolean metadata) {
      return new IterationStartOperation(codec, flags(), cfg, cacheNameBytes, topologyId, filterConverterFactory, filterParameters, segments, batchSize, transportFactory, metadata);
   }

   public IterationEndOperation newIterationEndOperation(String iterationId, Transport transport) {
      return new IterationEndOperation(codec, flags(), cfg, cacheNameBytes, topologyId, iterationId, transportFactory, transport);
   }

   public <K, V> IterationNextOperation newIterationNextOperation(String iterationId, Transport transport, KeyTracker segmentKeyTracker) {
      return new IterationNextOperation(codec, flags(), cfg, cacheNameBytes, topologyId, iterationId, transport, segmentKeyTracker, cfg.serialWhitelist());
   }

}
