package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.manager.CacheContainer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class OperationsFactory implements HotRodConstants {

   private static final Flag[] FORCE_RETURN_VALUE = {Flag.FORCE_RETURN_VALUE};

   private final ThreadLocal<Flag[]> flagsMap = new ThreadLocal<Flag[]>();

   private final TransportFactory transportFactory;

   private final byte[] cacheNameBytes;

   private final AtomicInteger topologyId;

   private final boolean forceReturnValue;

   public OperationsFactory(TransportFactory transportFactory, String cacheName, AtomicInteger topologyId, boolean forceReturnValue) {
      this.transportFactory = transportFactory;
      this.cacheNameBytes = cacheName.equals(CacheContainer.DEFAULT_CACHE_NAME) ? DEFAULT_CACHE_NAME_BYTES : cacheName.getBytes(HOTROD_STRING_CHARSET);
      this.topologyId = topologyId;
      this.forceReturnValue = forceReturnValue;
   }

   public GetOperation newGetKeyOperation(byte[] key) {
      return new GetOperation(transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public RemoveOperation newRemoveOperation(byte[] key) {
      return new RemoveOperation(transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public RemoveIfUnmodifiedOperation newRemoveIfUnmodifiedOperation(byte[] key, long version) {
      return new RemoveIfUnmodifiedOperation(transportFactory, key, cacheNameBytes, topologyId, flags(), version);
   }

   public ReplaceIfUnmodifiedOperation newReplaceIfUnmodifiedOperation(byte[] key, byte[] value, int lifespanSeconds, int maxIdleTimeSeconds, long version) {
      return new ReplaceIfUnmodifiedOperation(transportFactory, key, cacheNameBytes, topologyId, flags(), value, lifespanSeconds, maxIdleTimeSeconds, version);
   }

   public GetWithVersionOperation newGetWithVersionOperation(byte[] key) {
      return new GetWithVersionOperation(transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public StatsOperation newStatsOperation() {
      return new StatsOperation(transportFactory, cacheNameBytes, topologyId, flags());
   }

   public PutOperation newPutKeyValueOperation(byte[] key, byte[] value, int lifespanSecs, int maxIdleSecs) {
      return new PutOperation(transportFactory, key, cacheNameBytes, topologyId, flags(), value, lifespanSecs, maxIdleSecs);
   }

   public PutIfAbsentOperation newPutIfAbsentOperation(byte[] key, byte[] value, int lifespanSecs, int maxIdleSecs) {
      return new PutIfAbsentOperation(transportFactory, key, cacheNameBytes, topologyId, flags(), value, lifespanSecs, maxIdleSecs);
   }

   public ReplaceOperation newReplaceOperation(byte[] key, byte[] values, int lifespanSecs, int maxIdleSecs) {
      return new ReplaceOperation(transportFactory, key, cacheNameBytes, topologyId, flags(), values, lifespanSecs, maxIdleSecs);
   }

   public ContainsKeyOperation newContainsKeyOperation(byte[] key) {
      return new ContainsKeyOperation(transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(transportFactory, cacheNameBytes, topologyId, flags());
   }

   public BulkGetOperation newBulkGetOperation(int size) {
      return new BulkGetOperation(transportFactory, cacheNameBytes, topologyId, flags(), size);
   }

   public PingOperation newPingOperation() {
      return new PingOperation(topologyId, transportFactory.getTransport(), cacheNameBytes);
   }

   private Flag[] flags() {
      Flag[] flags = this.flagsMap.get();
      this.flagsMap.remove();
      if (flags == null && forceReturnValue) {
         return FORCE_RETURN_VALUE;
      }
      return flags;
   }

   public void setFlags(Flag[] flags) {
      this.flagsMap.set(flags);
   }
}
