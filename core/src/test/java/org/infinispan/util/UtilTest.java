package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.util.Util;
import org.testng.annotations.Test;

/**
 * Test utility methods
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "util.UtilTest")
public class UtilTest {

   public void testToHexString() {
      byte[] sample1 = {
            -36, -4, 27, 111, 3, 49, 118, -49, 55, 74, 25, 12, 57, -96, 17,
            -119, -30, -113, 98, -42, -52, 49, 74, 93, 44, -94, 39, -71, -54,
            37, 108, -102, -113, 82, 73, -88, 103, 88, -44, -49, -58, 127, -93,
            83, 32, 124, 18, 83, 35, -116, -38, 43, -26, -87, 20, -82, -29, 36,
            -20, 50, 97, -18, -30, 80};
      assertEquals("DCFC1B6F033176CF374A190C39A01189E28F62D6CC314A5D2CA227B9CA" +
                  "256C9A8F5249A86758D4CFC67FA353207C1253238CDA2BE6A914AEE324EC3261EEE250",
            Util.toHexString(sample1));

      byte[] sample2 = { -36, -4, 27, 111, 3, 49, 118 };
      assertEquals("DCFC1B6F033176", Util.toHexString(sample2, 8));

      byte[] sample3 = { -36, -4, 27, 111, 3, 49, 118, -49 };
      assertEquals("DCFC1B6F033176CF", Util.toHexString(sample3, 8));

      byte[] sample4 = { -36, -4, 27, 111, 3, 49, 118, -49, 55};
      assertEquals("DCFC1B6F033176CF", Util.toHexString(sample4, 8));
   }

}
