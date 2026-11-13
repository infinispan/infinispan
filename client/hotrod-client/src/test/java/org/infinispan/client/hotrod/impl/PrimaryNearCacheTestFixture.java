package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.AddPrimaryNearCacheClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation.GetWithMetadataResult;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.transport.netty.NearCacheTopologyListener;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.near.NearCacheService;

import io.netty.channel.Channel;

/** Runs real listener setup, with controllable transport completions and dispatcher notifications. */
@SuppressWarnings("unchecked")
class PrimaryNearCacheTestFixture {
   static final SocketAddress A = InetSocketAddress.createUnresolved("localhost", 11222);
   static final SocketAddress B = InetSocketAddress.createUnresolved("localhost", 11223);
   static final SocketAddress C = InetSocketAddress.createUnresolved("localhost", 11224);

   final InternalRemoteCache<Integer, String> remote = mock(InternalRemoteCache.class);
   final CacheOperationsFactory factory = mock(CacheOperationsFactory.class);
   final OperationDispatcher dispatcher = mock(OperationDispatcher.class);
   final ClientListenerNotifier notifier = mock(ClientListenerNotifier.class);
   final ExecutorService executor = mock(ExecutorService.class);
   final HotRodOperation<Void> removeOp = mock(HotRodOperation.class);
   final HotRodOperation<Void> updateOp = mock(HotRodOperation.class);
   final NearCacheService<Integer, String> near;
   final InvalidatedNearRemoteCache<Integer, String> cache;
   final Map<SocketAddress, Channel> channels = new HashMap<>();
   volatile CacheInfo latest;
   Object listener;
   byte[] listenerId;
   NearCacheTopologyListener topologyListener;
   Runnable codecChanged;

   PrimaryNearCacheTestFixture(boolean bloom) {
      when(remote.getOperationsFactory()).thenReturn(factory);
      when(factory.newFactoryFor(any())).thenReturn(factory);
      when(remote.getName()).thenReturn("");
      when(remote.keyToBytes(any())).thenAnswer(invocation -> new byte[]{(byte) (int) (Integer) invocation.getArgument(0)});
      when(remote.getDispatcher()).thenReturn(dispatcher);
      when(dispatcher.getClientListenerNotifier()).thenReturn(notifier);
      when(notifier.isListenerConnected(any())).thenReturn(true);
      when(notifier.findListenerId(any())).thenAnswer(invocation -> listenerId);
      when(dispatcher.isPrimaryNearCacheListenerSupported()).thenReturn(true);
      when(dispatcher.await(any())).thenAnswer(invocation ->
            ((CompletionStage<?>) invocation.getArgument(0)).toCompletableFuture().join());
      when(dispatcher.getCacheInfo("")).thenAnswer(invocation -> latest);
      when(dispatcher.getConsistentHash("")).thenAnswer(invocation -> latest == null ? null : latest.getConsistentHash());
      when(dispatcher.getPrimarySegmentsByAddress("")).thenAnswer(invocation -> latest == null ? null : latest.getPrimarySegments());
      when(dispatcher.withLatestTopology(eq(""), any())).thenAnswer(invocation ->
            ((Function<CacheInfo, Object>) invocation.getArgument(1)).apply(latest));
      when(dispatcher.primaryTopologyReady(eq(""), any())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(invocation.getArgument(1)));
      when(factory.newRemoveClientListenerOperation(any(byte[].class))).thenReturn(removeOp);
      when(factory.newUpdateBloomFilterOperation(any())).thenReturn(updateOp);
      when(factory.newAddPrimaryNearCacheListenerOperation(any(), any(byte[].class), anyInt())).thenAnswer(invocation -> {
         listener = invocation.getArgument(0);
         listenerId = invocation.getArgument(1);
         return new AddPrimaryNearCacheClientListenerOperation(remote, listener, listenerId, invocation.getArgument(2));
      });
      when(factory.newAddNearCacheListenerOperation(any(), anyInt())).thenReturn(mock(ClientListenerOperation.class));
      when(dispatcher.executeAddListener(any(ClientListenerOperation.class))).thenAnswer(invocation -> CompletableFuture.completedFuture(channel()));
      when(dispatcher.executeAddListener(any(ClientListenerOperation.class), any())).thenAnswer(invocation ->
            CompletableFuture.completedFuture(channels.computeIfAbsent(invocation.getArgument(1), ignored -> channel())));
      when(dispatcher.executeOnSingleAddress(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
      doAnswer(invocation -> {
         topologyListener = invocation.getArgument(1);
         return null;
      }).when(dispatcher).addTopologyListener(eq(""), any());
      doAnswer(invocation -> {
         codecChanged = invocation.getArgument(0);
         return null;
      }).when(dispatcher).addCodecChangeListener(any());
      when(dispatcher.getExecutorService()).thenReturn(executor);
      doAnswer(invocation -> {
         invocation.<Runnable>getArgument(0).run();
         return null;
      }).when(executor).execute(any(Runnable.class));
      when(executor.submit(any(Runnable.class))).thenAnswer(invocation ->
            CompletableFuture.runAsync(invocation.getArgument(0), executor));
      near = spy(NearCacheService.<Integer, String>create(new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, bloom), notifier));
      cache = new InvalidatedNearRemoteCache<>(remote, mock(ClientStatistics.class), near);
   }

   Channel start(SocketAddress... primaries) {
      topology(info(hash(primaries)));
      return near.start(cache);
   }

   void downgrade() {
      when(dispatcher.isPrimaryNearCacheListenerSupported()).thenReturn(false);
      codecChanged.run();
   }

   void topology(CacheInfo info) {
      latest = info;
   }

   void accept(CacheInfo next) {
      CacheInfo previous = latest;
      topologyListener.beforeTopologyUpdate(previous, next);
      topology(next);
      topologyListener.afterTopologyUpdate(previous, next);
   }

   void store(int key, String value) {
      var placeholder = near.createPlaceholder(key);
      assertThat(near.putIfAbsent(key, placeholder)).isTrue();
      assertThat(near.replace(key, placeholder, result(value).value())).isTrue();
   }

   void assertCacheable(int key, boolean cacheable) {
      when(remote.getWithMetadataAsync(eq(key), any())).thenReturn(CompletableFuture.completedFuture(result("v" + key)));
      assertThat(cache.getWithMetadataAsync(key).join().getValue()).isEqualTo("v" + key);
      if (cacheable) {
         assertThat(near.get(key)).as("Key %d should be cached", key).isNotNull();
      } else {
         assertThat(near.get(key)).as("Key %d must be fetched remotely", key).isNull();
      }
   }

   static Channel channel() {
      Channel channel = mock(Channel.class);
      when(channel.isActive()).thenReturn(true);
      return channel;
   }

   static GetWithMetadataResult<String> result(String value) {
      return new GetWithMetadataResult<>(new MetadataValueImpl<>(-1, -1, -1, -1, 1, value), false);
   }

   static SegmentConsistentHash hash(SocketAddress... primaries) {
      SegmentConsistentHash ch = mock(SegmentConsistentHash.class);
      SocketAddress[][] owners = new SocketAddress[primaries.length][];
      for (int i = 0; i < primaries.length; ++i) {
         owners[i] = new SocketAddress[]{primaries[i]};
      }
      when(ch.getNumSegments()).thenReturn(primaries.length);
      when(ch.getSegmentOwners()).thenReturn(owners);
      when(ch.getSegment(any())).thenAnswer(invocation -> Math.floorMod(((byte[]) invocation.getArgument(0))[0], primaries.length));
      when(ch.getServer(any())).thenAnswer(invocation -> primaries[ch.getSegment(invocation.getArgument(0))]);
      return ch;
   }

   static CacheInfo info(SegmentConsistentHash ch) {
      CacheInfo info = mock(CacheInfo.class);
      when(info.getConsistentHash()).thenReturn(ch);
      Map<SocketAddress, Set<Integer>> primarySegments = new HashMap<>();
      SocketAddress[][] owners = ch.getSegmentOwners();
      for (int segment = 0; segment < owners.length; ++segment) {
         primarySegments.computeIfAbsent(owners[segment][0], ignored -> new HashSet<>()).add(segment);
      }
      when(info.getPrimarySegments()).thenReturn(primarySegments);
      return info;
   }
}
