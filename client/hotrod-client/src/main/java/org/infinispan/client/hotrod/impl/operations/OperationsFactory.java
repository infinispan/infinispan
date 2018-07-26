package org.infinispan.client.hotrod.impl.operations;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class OperationsFactory implements HotRodConstants {

   private final ThreadLocal<Integer> flagsMap = new ThreadLocal<>();

   private final ChannelFactory channelFactory;

   private final byte[] cacheNameBytes;

   private final AtomicInteger topologyId;

   private final boolean forceReturnValue;

   private final Codec codec;

   private final ClientListenerNotifier listenerNotifier;

   private final String cacheName;

   private final Configuration cfg;

   public OperationsFactory(ChannelFactory channelFactory, String cacheName, boolean forceReturnValue, Codec
         codec, ClientListenerNotifier listenerNotifier, Configuration cfg) {
      this.channelFactory = channelFactory;
      this.cacheNameBytes = cacheName == null ? DEFAULT_CACHE_NAME_BYTES : RemoteCacheManager.cacheNameBytes(cacheName);
      this.cacheName = cacheName;
      this.topologyId = channelFactory != null
            ? channelFactory.createTopologyId(cacheNameBytes)
            : new AtomicInteger(-1);
      this.forceReturnValue = forceReturnValue;
      this.codec = codec;
      this.listenerNotifier = listenerNotifier;
      this.cfg = cfg;
   }

   public OperationsFactory(ChannelFactory channelFactory, Codec codec, Configuration cfg, ClientListenerNotifier listenerNotifier) {
      this(channelFactory, null, false, codec, listenerNotifier, cfg);
   }

   public ClientListenerNotifier getListenerNotifier() {
      return listenerNotifier;
   }

   public String getCacheName() {
      return cacheName;
   }

   public Codec getCodec() {
      return codec;
   }

   public <V> GetOperation<V> newGetKeyOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new GetOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat);
   }

   public <K, V> GetAllParallelOperation<K, V> newGetAllOperation(Set<byte[]> keys, DataFormat dataFormat) {
      return new GetAllParallelOperation<>(codec, channelFactory, keys, cacheNameBytes, topologyId, flags(),
            cfg, dataFormat);
   }

   public <V> RemoveOperation<V> newRemoveOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new RemoveOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat);
   }

   public <V> RemoveIfUnmodifiedOperation<V> newRemoveIfUnmodifiedOperation(Object key, byte[] keyBytes, long version, DataFormat dataFormat) {
      return new RemoveIfUnmodifiedOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, version, dataFormat);
   }

   public ReplaceIfUnmodifiedOperation newReplaceIfUnmodifiedOperation(Object key, byte[] keyBytes,
                                                                       byte[] value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version, DataFormat dataFormat) {
      return new ReplaceIfUnmodifiedOperation(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version, dataFormat);
   }

   public <V> GetWithVersionOperation<V> newGetWithVersionOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new GetWithVersionOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat);
   }

   public <V> GetWithMetadataOperation<V> newGetWithMetadataOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new GetWithMetadataOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat);
   }

   public StatsOperation newStatsOperation() {
      return new StatsOperation(
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <V> PutOperation<V> newPutKeyValueOperation(Object key, byte[] keyBytes, byte[] value,
                                                      long lifespan, TimeUnit lifespanTimeUnit, long maxIdle,
                                                      TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat);
   }

   public PutAllParallelOperation newPutAllOperation(Map<byte[], byte[]> map,
                                                     long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutAllParallelOperation(
            codec, channelFactory, map, cacheNameBytes, topologyId, flags(lifespan, maxIdle), cfg,
            lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat);
   }

   public <V> PutIfAbsentOperation<V> newPutIfAbsentOperation(Object key, byte[] keyBytes, byte[] value,
                                                              long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
                                                              TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new PutIfAbsentOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdleTime),
            cfg, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, dataFormat);
   }

   public <V> ReplaceOperation<V> newReplaceOperation(Object key, byte[] keyBytes, byte[] values,
                                                      long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat) {
      return new ReplaceOperation<>(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(lifespan, maxIdle),
            cfg, values, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat);
   }

   public ContainsKeyOperation newContainsKeyOperation(Object key, byte[] keyBytes, DataFormat dataFormat) {
      return new ContainsKeyOperation(
            codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, dataFormat);
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <K, V> BulkGetOperation<K, V> newBulkGetOperation(int size) {
      return new BulkGetOperation(
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg, size);
   }

   public <K> BulkGetKeysOperation<K> newBulkGetKeysOperation(int scope) {
      return new BulkGetKeysOperation<>(
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg, scope);
   }

   public AddClientListenerOperation newAddClientListenerOperation(Object listener, DataFormat dataFormat) {
      return new AddClientListenerOperation(codec, channelFactory,
            cacheName, topologyId, flags(), cfg, listenerNotifier,
            listener, null, null, dataFormat);
   }

   public AddClientListenerOperation newAddClientListenerOperation(
         Object listener, byte[][] filterFactoryParams, byte[][] converterFactoryParams, DataFormat dataFormat) {
      return new AddClientListenerOperation(codec, channelFactory,
            cacheName, topologyId, flags(), cfg, listenerNotifier,
            listener, filterFactoryParams, converterFactoryParams, dataFormat);
   }

   public RemoveClientListenerOperation newRemoveClientListenerOperation(Object listener) {
      return new RemoveClientListenerOperation(codec, channelFactory,
            cacheNameBytes, topologyId, flags(), cfg, listenerNotifier, listener);
   }

   /**
    * Construct a ping request directed to a particular node.
    *
    * @return a ping operation for a particular node
    * @param releaseChannel
    */
   public PingOperation newPingOperation(boolean releaseChannel) {
      return new PingOperation(codec, topologyId, cfg, cacheNameBytes, channelFactory, releaseChannel);
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
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public QueryOperation newQueryOperation(RemoteQuery remoteQuery, DataFormat dataFormat) {
      return new QueryOperation(
            codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg, remoteQuery, dataFormat);
   }

   public SizeOperation newSizeOperation() {
      return new SizeOperation(codec, channelFactory, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <T> ExecuteOperation<T> newExecuteOperation(String taskName, Map<String, byte[]> marshalledParams, Object key, DataFormat dataFormat) {
      return new ExecuteOperation<>(codec, channelFactory, cacheNameBytes,
            topologyId, flags(), cfg, taskName, marshalledParams, key, dataFormat);
   }

   public AdminOperation newAdminOperation(String taskName, Map<String, byte[]> marshalledParams) {
      return new AdminOperation(codec, channelFactory, cacheNameBytes,
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
      for (Flag flag : flags)
         intFlags |= flag.getFlagInt();
      this.flagsMap.set(intFlags);
   }

   public void setFlags(int intFlags) {
      this.flagsMap.set(intFlags);
   }

   public boolean hasFlag(Flag flag) {
      Integer threadLocalFlags = this.flagsMap.get();
      return threadLocalFlags != null && (threadLocalFlags & flag.getFlagInt()) != 0;
   }

   public CacheTopologyInfo getCacheTopologyInfo() {
      return channelFactory.getCacheTopologyInfo(cacheNameBytes);
   }

   public IterationStartOperation newIterationStartOperation(String filterConverterFactory, byte[][] filterParameters, Set<Integer> segments, int batchSize, boolean metadata, DataFormat dataFormat) {
      return new IterationStartOperation(codec, flags(), cfg, cacheNameBytes, topologyId, filterConverterFactory, filterParameters, segments, batchSize, channelFactory, metadata, dataFormat);
   }

   public IterationEndOperation newIterationEndOperation(byte[] iterationId, Channel channel) {
      return new IterationEndOperation(codec, flags(), cfg, cacheNameBytes, topologyId, iterationId, channelFactory, channel);
   }

   public <E> IterationNextOperation<E> newIterationNextOperation(byte[] iterationId, Channel channel, KeyTracker segmentKeyTracker, DataFormat dataFormat) {
      return new IterationNextOperation(codec, flags(), cfg, cacheNameBytes, topologyId, iterationId, channel, channelFactory, segmentKeyTracker, dataFormat);
   }

   public <K> GetStreamOperation newGetStreamOperation(K key, byte[] keyBytes, int offset) {
      return new GetStreamOperation(codec, channelFactory, key, keyBytes, offset, cacheNameBytes, topologyId, flags(), cfg);
   }

   public <K> PutStreamOperation newPutStreamOperation(K key, byte[] keyBytes, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public <K> PutStreamOperation newPutStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, PutStreamOperation.VERSION_PUT, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public <K> PutStreamOperation newPutIfAbsentStreamOperation(K key, byte[] keyBytes, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return new PutStreamOperation(codec, channelFactory, key, keyBytes, cacheNameBytes, topologyId, flags(), cfg, PutStreamOperation.VERSION_PUT_IF_ABSENT, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public AuthMechListOperation newAuthMechListOperation(Channel channel) {
      return new AuthMechListOperation(codec, topologyId, cfg, channel, channelFactory);
   }

   public AuthOperation newAuthOperation(Channel channel, String saslMechanism, byte[] response) {
      return new AuthOperation(codec, topologyId, cfg, channel, channelFactory, saslMechanism, response);
   }

   public PrepareTransactionOperation newPrepareTransactionOperation(Xid xid, boolean onePhaseCommit,
                                                                     Collection<Modification> modifications) {
      return new PrepareTransactionOperation(codec, channelFactory, cacheNameBytes, topologyId, cfg, xid,
            onePhaseCommit, modifications);
   }

   public CompleteTransactionOperation newCompleteTransactionOperation(Xid xid, boolean commit) {
      return new CompleteTransactionOperation(codec, channelFactory, cacheNameBytes, topologyId, cfg, xid, commit);
   }
}
