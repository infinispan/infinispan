package org.infinispan.server.resp.commands.topk;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.testng.annotations.Test;

/**
 * Unit tests for the TopK data structure.
 *
 * @since 16.2
 */
@Test(groups = "unit", testName = "server.resp.commands.topk.TopKUnitTest")
public class TopKUnitTest {

   private static byte[] bytes(String s) {
      return s.getBytes(StandardCharsets.UTF_8);
   }

   @Test
   public void testAddAndQuery() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.add(bytes("a"));
      topK.add(bytes("b"));
      topK.add(bytes("c"));

      assertThat(topK.query(bytes("a"))).isTrue();
      assertThat(topK.query(bytes("b"))).isTrue();
      assertThat(topK.query(bytes("c"))).isTrue();
      assertThat(topK.query(bytes("d"))).isFalse();
   }

   @Test
   public void testAddReturnsExpelledItem() {
      TopK topK = new TopK(2, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      assertThat(topK.add(bytes("a"))).isNull();
      assertThat(topK.add(bytes("b"))).isNull();

      // Adding items with higher count to force expulsion
      topK.incrBy(bytes("a"), 10);
      topK.incrBy(bytes("b"), 5);

      // "c" with count 1 should not expel anything since both a and b have higher counts
      byte[] expelled = topK.add(bytes("c"));
      assertThat(expelled).isNull();
   }

   @Test
   public void testIncrByExpelsMinimum() {
      TopK topK = new TopK(2, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.add(bytes("a"));
      topK.add(bytes("b"));

      // Both have count 1, incr "c" by 10 should expel one of a or b
      byte[] expelled = topK.incrBy(bytes("c"), 10);
      assertThat(expelled).isNotNull();
      assertThat(topK.query(bytes("c"))).isTrue();
   }

   @Test
   public void testIncrByExistingItem() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.add(bytes("a"));
      byte[] expelled = topK.incrBy(bytes("a"), 5);

      assertThat(expelled).isNull();
      assertThat(topK.getCount(bytes("a"))).isEqualTo(6);
   }

   @Test
   public void testGetCount() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      assertThat(topK.getCount(bytes("a"))).isEqualTo(0);

      topK.add(bytes("a"));
      assertThat(topK.getCount(bytes("a"))).isEqualTo(1);

      topK.incrBy(bytes("a"), 9);
      assertThat(topK.getCount(bytes("a"))).isEqualTo(10);
   }

   @Test
   public void testListWithoutCount() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.incrBy(bytes("c"), 1);
      topK.incrBy(bytes("a"), 10);
      topK.incrBy(bytes("b"), 5);

      List<Object> list = topK.list(false);
      assertThat(list).hasSize(3);
      // Sorted by count descending
      assertThat(list.get(0)).isEqualTo("a");
      assertThat(list.get(1)).isEqualTo("b");
      assertThat(list.get(2)).isEqualTo("c");
   }

   @Test
   public void testListWithCount() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.incrBy(bytes("a"), 10);
      topK.incrBy(bytes("b"), 5);
      topK.incrBy(bytes("c"), 1);

      List<Object> list = topK.list(true);
      assertThat(list).hasSize(6);
      assertThat(list.get(0)).isEqualTo("a");
      assertThat(list.get(1)).isEqualTo(10L);
      assertThat(list.get(2)).isEqualTo("b");
      assertThat(list.get(3)).isEqualTo(5L);
      assertThat(list.get(4)).isEqualTo("c");
      assertThat(list.get(5)).isEqualTo(1L);
   }

   @Test
   public void testEmptyTopK() {
      TopK topK = new TopK(5, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      assertThat(topK.query(bytes("anything"))).isFalse();
      assertThat(topK.getCount(bytes("anything"))).isEqualTo(0);
      assertThat(topK.list(false)).isEmpty();
      assertThat(topK.list(true)).isEmpty();
   }

   @Test
   public void testParameters() {
      TopK topK = new TopK(10, 16, 5, 0.8);

      assertThat(topK.getK()).isEqualTo(10);
      assertThat(topK.getWidth()).isEqualTo(16);
      assertThat(topK.getDepth()).isEqualTo(5);
      assertThat(topK.getDecay()).isEqualTo(0.8);
   }

   @Test
   public void testProtoStreamRoundTrip() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.incrBy(bytes("a"), 10);
      topK.incrBy(bytes("b"), 5);
      topK.incrBy(bytes("c"), 1);

      // Simulate ProtoStream serialization/deserialization
      long[] flat = topK.getFlatCounters();
      List<TopK.TopKEntry> entries = topK.getEntries();

      TopK restored = new TopK(topK.getK(), topK.getWidth(), topK.getDepth(), topK.getDecay(),
            flat, entries);

      assertThat(restored.query(bytes("a"))).isTrue();
      assertThat(restored.query(bytes("b"))).isTrue();
      assertThat(restored.query(bytes("c"))).isTrue();
      assertThat(restored.getCount(bytes("a"))).isEqualTo(10);
      assertThat(restored.getCount(bytes("b"))).isEqualTo(5);
      assertThat(restored.getCount(bytes("c"))).isEqualTo(1);
   }

   @Test
   public void testMultipleAddsTrackTopItems() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      for (int i = 0; i < 10; i++) {
         topK.incrBy(bytes("item" + i), i + 1);
      }

      // Top 3 should be items with highest counts: item9(10), item8(9), item7(8)
      assertThat(topK.query(bytes("item9"))).isTrue();
      assertThat(topK.query(bytes("item8"))).isTrue();
      assertThat(topK.query(bytes("item7"))).isTrue();
   }

   @Test
   public void testDuplicateAddIncrementsCount() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      topK.add(bytes("a"));
      topK.add(bytes("a"));
      topK.add(bytes("a"));

      assertThat(topK.getCount(bytes("a"))).isEqualTo(3);
   }

   @Test
   public void testByteArrayEquality() {
      TopK topK = new TopK(3, TopK.DEFAULT_WIDTH, TopK.DEFAULT_DEPTH, TopK.DEFAULT_DECAY);

      byte[] item1 = bytes("hello");
      byte[] item2 = bytes("hello");

      topK.add(item1);
      // Same content, different array instance
      assertThat(topK.query(item2)).isTrue();
      assertThat(topK.getCount(item2)).isEqualTo(1);

      topK.incrBy(item2, 5);
      assertThat(topK.getCount(item1)).isEqualTo(6);
   }
}
