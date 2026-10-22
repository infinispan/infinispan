package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.impl.InvalidatedNearTopologyBoundsTest", groups = "unit")
public class InvalidatedNearTopologyBoundsTest {

   @Test
   public void testTopologyUpdateWithTruncatedSegmentOwnersDoesNotThrow() {
      @SuppressWarnings("unchecked")
      InternalRemoteCache<Object, Object> rci = mock(InternalRemoteCache.class);
      CacheOperationsFactory cof = mock(CacheOperationsFactory.class);
      when(rci.getOperationsFactory()).thenReturn(cof);
      when(cof.newFactoryFor(any())).thenReturn(cof);

      NearCacheConfiguration cfg = new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, false);
      NearCacheService<Object, Object> nearcache = NearCacheService.create(cfg, null);

      InvalidatedNearRemoteCache<Object, Object> inrc = new InvalidatedNearRemoteCache<>(rci, null, nearcache);
      AtomicBoolean inPrimaryMode = TestingUtil.extractField(inrc, "inPrimaryMode");
      inPrimaryMode.set(true);

      SocketAddress s0 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress s1 = InetSocketAddress.createUnresolved("127.0.0.1", 11223);

      // Old consistent hash has 4 segments with valid owners for all segments
      SocketAddress[][] oldOwners = new SocketAddress[][]{{s0}, {s1}, {s0}, {s1}};
      SegmentConsistentHash oldSCH = new SegmentConsistentHash();
      oldSCH.init(oldOwners, 4);

      // New consistent hash reports 4 segments, but its owners array is truncated to length 2 (malformed/truncated topology)
      SocketAddress[][] newOwners = new SocketAddress[][]{{s0}, {s0}};
      SegmentConsistentHash newSCH = new SegmentConsistentHash();
      newSCH.init(newOwners, 4);

      List<InetSocketAddress> servers = List.of((InetSocketAddress) s0, (InetSocketAddress) s1);
      CacheInfo oldInfo = new CacheInfo("test", null, servers, ClientIntelligence.HASH_DISTRIBUTION_AWARE)
            .withNewHash(1, servers, oldSCH, 4);
      CacheInfo newInfo = oldInfo.withNewHash(2, servers, newSCH, 4);

      // Invoking onTopologyUpdate must not throw ArrayIndexOutOfBoundsException
      inrc.onTopologyUpdate(oldInfo, newInfo);

      AtomicIntegerArray epochs = TestingUtil.extractField(nearcache, "segmentEpochs");
      assertThat(epochs).isNotNull();
      assertThat(epochs.length()).isEqualTo(4);

      // Segment 0: remained s0 -> epoch unchanged (0)
      assertThat(epochs.get(0)).as("Segment 0 owner was unchanged").isEqualTo(0);
      // Segment 1: changed from s1 to s0 -> invalidated (1)
      assertThat(epochs.get(1)).as("Segment 1 owner changed").isEqualTo(1);
      // Segments 2 and 3: truncated in newOwners -> must be invalidated
      assertThat(epochs.get(2)).as("Segment 2 truncated in newOwners must be invalidated").isEqualTo(1);
      assertThat(epochs.get(3)).as("Segment 3 truncated in newOwners must be invalidated").isEqualTo(1);
   }

   @Test
   public void testTopologyUpdateWithEmptyNewOwnersDoesNotThrow() {
      @SuppressWarnings("unchecked")
      InternalRemoteCache<Object, Object> rci = mock(InternalRemoteCache.class);
      CacheOperationsFactory cof = mock(CacheOperationsFactory.class);
      when(rci.getOperationsFactory()).thenReturn(cof);
      when(cof.newFactoryFor(any())).thenReturn(cof);

      NearCacheConfiguration cfg = new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, false);
      NearCacheService<Object, Object> nearcache = NearCacheService.create(cfg, null);

      InvalidatedNearRemoteCache<Object, Object> inrc = new InvalidatedNearRemoteCache<>(rci, null, nearcache);
      AtomicBoolean inPrimaryMode = TestingUtil.extractField(inrc, "inPrimaryMode");
      inPrimaryMode.set(true);

      SocketAddress s0 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      List<InetSocketAddress> servers = List.of((InetSocketAddress) s0);

      SocketAddress[][] oldOwners = new SocketAddress[][]{{s0}, {s0}};
      SegmentConsistentHash oldSCH = new SegmentConsistentHash();
      oldSCH.init(oldOwners, 2);

      // Empty owners array
      SocketAddress[][] emptyOwners = new SocketAddress[0][0];
      SegmentConsistentHash emptySCH = new SegmentConsistentHash();
      emptySCH.init(emptyOwners, 2);

      CacheInfo oldInfo = new CacheInfo("test", null, servers, ClientIntelligence.HASH_DISTRIBUTION_AWARE)
            .withNewHash(1, servers, oldSCH, 2);
      CacheInfo newInfo = oldInfo.withNewHash(2, servers, emptySCH, 2);

      inrc.onTopologyUpdate(oldInfo, newInfo);

      AtomicIntegerArray epochs = TestingUtil.extractField(nearcache, "segmentEpochs");
      assertThat(epochs).isNotNull();
      assertThat(epochs.length()).isEqualTo(2);
      assertThat(epochs.get(0)).isEqualTo(1);
      assertThat(epochs.get(1)).isEqualTo(1);
   }
}
