package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "container.offheap.UnpooledOffHeapMemoryAllocatorTest")
public class UnpooledOffHeapMemoryAllocatorTest {

   @DataProvider(name = "roundings")
   Object[][] roundings() {
      return new Object[][] {
            { 2, 16 },
            { 14, 32 },
            { 23, 32 },
            { 32, 48 },
            { 123, 144 },
            { 43, 64 },
            { 73, 96 },
      };
   }

   @Test(dataProvider = "roundings")
   public void testRoundings(long original, long expected) {
      // The estimate adds 8 and rounds up to 16
      assertEquals(expected, UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(original));
   }
}
