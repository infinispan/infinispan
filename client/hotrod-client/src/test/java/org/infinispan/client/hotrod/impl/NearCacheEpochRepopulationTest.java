package org.infinispan.client.hotrod.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "client.hotrod.impl.NearCacheEpochRepopulationTest")
@SuppressWarnings("unchecked")
public class NearCacheEpochRepopulationTest extends AbstractInfinispanTest {
   public void testRefillingOneKeySurvivesEvictingAnotherStaleKey() {
      ClientListenerNotifier notifier = mock(ClientListenerNotifier.class);
      when(notifier.isListenerConnected(any())).thenReturn(true);
      NearCacheService<Integer, String> near = NearCacheService.create(
            new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, false), notifier);
      near.start(mock(InvalidatedNearRemoteCache.class));
      near.setSegmentFunction(key -> (Integer) key % 2, 2);
      populate(near, 1, "old-1");
      populate(near, 3, "old-3");
      populate(near, 2, "unaffected");

      near.invalidateSegment(1);
      assertThat(near.get(1)).isNull();
      populate(near, 1, "fresh-1");
      assertThat(near.get(3)).isNull();
      assertThat(near.get(1).getValue())
            .as("Refilling a key must survive lazy removal of another stale key in the same segment")
            .isEqualTo("fresh-1");
      populate(near, 3, "fresh-3");
      assertThat(near.get(1).getValue()).isEqualTo("fresh-1");
      assertThat(near.get(3).getValue()).isEqualTo("fresh-3");
      assertThat(near.get(2).getValue()).as("Other segments must stay cached").isEqualTo("unaffected");
   }

   public void testBloomCalculationRemovesStaleValuesAndPlaceholders() {
      NearCacheService<Integer, String> near = bloomNearCache();
      near.setSegmentFunction(key -> (Integer) key % 2, 2);
      populate(near, 1, "stale");
      var stalePlaceholder = near.createPlaceholder(3);
      assertThat(near.putIfAbsent(3, stalePlaceholder)).isTrue();
      populate(near, 2, "unaffected");

      near.invalidateSegment(1);
      populate(near, 5, "fresh");
      var currentPlaceholder = near.createPlaceholder(7);
      assertThat(near.putIfAbsent(7, currentPlaceholder)).isTrue();

      assertThat(near.calculateBloomBits()).containsExactly(bloomBits(near, 2, 5, 7));
      assertThat(near.size()).as("Stale entries must be physically removed during calculation").isEqualTo(3);
      assertThat(near.replace(3, stalePlaceholder, new MetadataValueImpl<>(-1, -1, -1, -1, 1, "late"))).isFalse();
      assertThat(near.get(2).getValue()).isEqualTo("unaffected");
      assertThat(near.get(5).getValue()).isEqualTo("fresh");
      assertThat(near.replace(7, currentPlaceholder, new MetadataValueImpl<>(-1, -1, -1, -1, 1, "completed"))).isTrue();
      assertThat(near.get(7).getValue()).isEqualTo("completed");
   }

   public void testBloomCalculationRemovesEntriesFromPreviousMapping() {
      NearCacheService<Integer, String> near = bloomNearCache();
      near.setSegmentFunction(key -> (Integer) key % 2, 2);
      populate(near, 1, "old-mapping");
      near.setSegmentFunction(key -> (Integer) key % 4, 4);
      populate(near, 2, "new-mapping");

      assertThat(near.calculateBloomBits()).containsExactly(bloomBits(near, 2));
      assertThat(near.size()).isEqualTo(1);
      assertThat(near.get(2).getValue()).isEqualTo("new-mapping");
   }

   private NearCacheService<Integer, String> bloomNearCache() {
      ClientListenerNotifier notifier = mock(ClientListenerNotifier.class);
      when(notifier.isListenerConnected(any())).thenReturn(true);
      NearCacheService<Integer, String> near = NearCacheService.create(
            new NearCacheConfiguration(NearCacheMode.INVALIDATED, 100, true), notifier);
      InvalidatedNearRemoteCache<Integer, String> remote = mock(InvalidatedNearRemoteCache.class);
      when(remote.keyToBytes(any())).thenAnswer(invocation -> new byte[]{((Integer) invocation.getArgument(0)).byteValue()});
      near.start(remote);
      return near;
   }

   private byte[] bloomBits(NearCacheService<Integer, String> near, int... keys) {
      var filter = MurmurHash3BloomFilter.createFilter(near.getBloomFilterBits());
      for (int key : keys) {
         filter.addToFilter(new byte[]{(byte) key});
      }
      return filter.getIntSet().toBitSet();
   }

   private void populate(NearCacheService<Integer, String> near, int key, String value) {
      var placeholder = near.createPlaceholder(key);
      assertThat(near.putIfAbsent(key, placeholder)).isTrue();
      assertThat(near.replace(key, placeholder, new MetadataValueImpl<>(-1, -1, -1, -1, 1, value))).isTrue();
   }
}
