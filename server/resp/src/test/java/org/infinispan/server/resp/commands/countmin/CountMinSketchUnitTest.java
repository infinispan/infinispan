package org.infinispan.server.resp.commands.countmin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

/**
 * Unit tests for the CountMinSketch data structure.
 *
 * @since 16.2
 */
@Test(groups = "unit", testName = "server.resp.commands.countmin.CountMinSketchUnitTest")
public class CountMinSketchUnitTest {

   @Test
   public void testIncrByAndQuery() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(cms.query(item)).isEqualTo(0);

      cms.incrBy(item, 5);
      assertThat(cms.query(item)).isEqualTo(5);

      cms.incrBy(item, 3);
      assertThat(cms.query(item)).isEqualTo(8);
   }

   @Test
   public void testIncrByReturnsMinCount() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      long count = cms.incrBy(item, 10);
      assertThat(count).isEqualTo(10);

      count = cms.incrBy(item, 5);
      assertThat(count).isEqualTo(15);
   }

   @Test
   public void testTotalCount() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      assertThat(cms.getTotalCount()).isEqualTo(0);

      cms.incrBy("a".getBytes(StandardCharsets.UTF_8), 3);
      cms.incrBy("b".getBytes(StandardCharsets.UTF_8), 7);

      assertThat(cms.getTotalCount()).isEqualTo(10);
   }

   @Test
   public void testQueryUnknownItem() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      cms.incrBy("known".getBytes(StandardCharsets.UTF_8), 100);

      byte[] unknown = "unknown".getBytes(StandardCharsets.UTF_8);
      assertThat(cms.query(unknown)).isEqualTo(0);
   }

   @Test
   public void testMultipleItems() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      for (int i = 0; i < 100; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         cms.incrBy(item, i + 1);
      }

      for (int i = 0; i < 100; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         assertThat(cms.query(item)).isGreaterThanOrEqualTo(i + 1);
      }

      assertThat(cms.getTotalCount()).isEqualTo(5050);
   }

   @Test
   public void testOvercountingProperty() {
      CountMinSketch cms = new CountMinSketch(100, 3);

      for (int i = 0; i < 1000; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         cms.incrBy(item, 1);
      }

      for (int i = 0; i < 1000; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         assertThat(cms.query(item)).isGreaterThanOrEqualTo(1);
      }
   }

   @Test
   public void testMerge() {
      CountMinSketch cms1 = new CountMinSketch(2000, 7);
      CountMinSketch cms2 = new CountMinSketch(2000, 7);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      cms1.incrBy(item, 5);
      cms2.incrBy(item, 3);

      cms1.merge(cms2, 1.0);
      assertThat(cms1.query(item)).isEqualTo(8);
      assertThat(cms1.getTotalCount()).isEqualTo(8);
   }

   @Test
   public void testMergeWithWeight() {
      CountMinSketch cms1 = new CountMinSketch(2000, 7);
      CountMinSketch cms2 = new CountMinSketch(2000, 7);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      cms1.incrBy(item, 10);
      cms2.incrBy(item, 10);

      cms1.merge(cms2, 2.0);
      assertThat(cms1.query(item)).isEqualTo(30);
   }

   @Test
   public void testMergeDimensionMismatch() {
      CountMinSketch cms1 = new CountMinSketch(2000, 7);
      CountMinSketch cms2 = new CountMinSketch(1000, 7);

      assertThatThrownBy(() -> cms1.merge(cms2, 1.0))
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testDimensions() {
      CountMinSketch cms = new CountMinSketch(500, 5);

      assertThat(cms.getWidth()).isEqualTo(500);
      assertThat(cms.getDepth()).isEqualTo(5);
      assertThat(cms.getCounters()).hasSize(2500);
   }

   @Test
   public void testEmptySketch() {
      CountMinSketch cms = new CountMinSketch(2000, 7);

      assertThat(cms.getTotalCount()).isEqualTo(0);
      assertThat(cms.getWidth()).isEqualTo(2000);
      assertThat(cms.getDepth()).isEqualTo(7);
      assertThat(cms.query("anything".getBytes(StandardCharsets.UTF_8))).isEqualTo(0);
   }
}
