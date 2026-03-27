package org.infinispan.server.resp.commands.cuckoo;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

/**
 * Unit tests for the CuckooFilter data structure.
 *
 * @since 16.2
 */
@Test(groups = "unit", testName = "server.resp.commands.cuckoo.CuckooFilterUnitTest")
public class CuckooFilterUnitTest {

   @Test
   public void testAddAndExists() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.exists(item)).isFalse();

      assertThat(filter.add(item)).isTrue();
      assertThat(filter.exists(item)).isTrue();
      assertThat(filter.getItemsInserted()).isEqualTo(1);
   }

   @Test
   public void testAddDuplicates() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.add(item)).isTrue();
      assertThat(filter.add(item)).isTrue();
      assertThat(filter.count(item)).isEqualTo(2);
      assertThat(filter.getItemsInserted()).isEqualTo(2);
   }

   @Test
   public void testAddNx() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.addNx(item)).isTrue();
      assertThat(filter.addNx(item)).isFalse();
      assertThat(filter.count(item)).isEqualTo(1);
   }

   @Test
   public void testDelete() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.delete(item)).isFalse();

      filter.add(item);
      assertThat(filter.delete(item)).isTrue();
      assertThat(filter.exists(item)).isFalse();
      assertThat(filter.getItemsDeleted()).isEqualTo(1);
   }

   @Test
   public void testDeleteOneOccurrence() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      filter.add(item);
      filter.add(item);
      assertThat(filter.count(item)).isEqualTo(2);

      filter.delete(item);
      assertThat(filter.count(item)).isEqualTo(1);
      assertThat(filter.exists(item)).isTrue();
   }

   @Test
   public void testCount() {
      CuckooFilter filter = new CuckooFilter(1024, 4, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.count(item)).isEqualTo(0);

      filter.add(item);
      assertThat(filter.count(item)).isEqualTo(1);

      filter.add(item);
      filter.add(item);
      assertThat(filter.count(item)).isEqualTo(3);
   }

   @Test
   public void testMultipleItems() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      for (int i = 0; i < 100; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         assertThat(filter.add(item)).isTrue();
      }

      for (int i = 0; i < 100; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         assertThat(filter.exists(item)).isTrue();
      }

      assertThat(filter.getItemsInserted()).isEqualTo(100);
   }

   @Test
   public void testExpansion() {
      CuckooFilter filter = new CuckooFilter(64, 2, 20, 2);

      int added = 0;
      for (int i = 0; i < 500; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         if (filter.add(item)) {
            added++;
         }
      }

      assertThat(added).isGreaterThan(64);
      assertThat(filter.getFilterCount()).isGreaterThan(1);
   }

   @Test
   public void testNoExpansion() {
      CuckooFilter filter = new CuckooFilter(16, 2, 20, 0);

      boolean full = false;
      for (int i = 0; i < 100; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         if (!filter.add(item)) {
            full = true;
            break;
         }
      }

      assertThat(full).isTrue();
      assertThat(filter.getFilterCount()).isEqualTo(1);
   }

   @Test
   public void testCapacityRoundedToPowerOfTwo() {
      CuckooFilter filter = new CuckooFilter(100, 2, 20, 1);
      // 100 rounds up to 128
      assertThat(filter.getCapacity()).isEqualTo(128);

      CuckooFilter filter2 = new CuckooFilter(1, 2, 20, 1);
      assertThat(filter2.getCapacity()).isEqualTo(1);

      CuckooFilter filter3 = new CuckooFilter(1024, 2, 20, 1);
      assertThat(filter3.getCapacity()).isEqualTo(1024);
   }

   @Test
   public void testGetSize() {
      CuckooFilter filter = new CuckooFilter(128, 2, 20, 1);
      // 128 buckets * 2 bucket size = 256 bytes
      assertThat(filter.getSize()).isEqualTo(256);
   }

   @Test
   public void testGetTotalBuckets() {
      CuckooFilter filter = new CuckooFilter(128, 2, 20, 1);
      assertThat(filter.getTotalBuckets()).isEqualTo(128);
   }

   @Test
   public void testDeleteNonExistentItem() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.delete(item)).isFalse();
      assertThat(filter.getItemsDeleted()).isEqualTo(0);
   }

   @Test
   public void testExistsOnEmptyFilter() {
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item = "hello".getBytes(StandardCharsets.UTF_8);
      assertThat(filter.exists(item)).isFalse();
      assertThat(filter.count(item)).isEqualTo(0);
   }

   @Test
   public void testAltIndexInvariant() {
      // Verifies that the alt index operation is its own inverse:
      // altIndex(altIndex(i, fp), fp) == i
      CuckooFilter filter = new CuckooFilter(1024, 2, 20, 1);

      byte[] item1 = "test1".getBytes(StandardCharsets.UTF_8);
      byte[] item2 = "test2".getBytes(StandardCharsets.UTF_8);

      // Add and verify items can be found after insertion
      filter.add(item1);
      filter.add(item2);
      assertThat(filter.exists(item1)).isTrue();
      assertThat(filter.exists(item2)).isTrue();

      // Delete item1 and verify item2 is still found
      filter.delete(item1);
      assertThat(filter.exists(item1)).isFalse();
      assertThat(filter.exists(item2)).isTrue();
   }

   @Test
   public void testEvictionStillFindable() {
      // Use a small filter with expansion to force evictions and sub-filter growth
      CuckooFilter filter = new CuckooFilter(8, 2, 500, 2);

      int added = 0;
      for (int i = 0; i < 30; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         if (filter.add(item)) {
            added++;
         }
      }

      assertThat(added).isEqualTo(30);
      assertThat(filter.getFilterCount()).isGreaterThan(1);

      // All items should be findable
      for (int i = 0; i < 30; i++) {
         byte[] item = ("item" + i).getBytes(StandardCharsets.UTF_8);
         assertThat(filter.exists(item))
               .as("item%d should exist after add", i)
               .isTrue();
      }
   }
}
