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
      assertEquals("dcfc1b6f033176cf374a190c39a01189e28f62d6cc314a5d2ca227b9ca" +
            "256c9a8f5249a86758d4cfc67fa353207c1253238cda2be6a914aee324ec3261eee250",
            Util.toHexString(sample1));

      byte[] sample2 = { -36, -4, 27, 111, 3, 49, 118 };
      assertEquals("dcfc1b6f033176", Util.toHexString(sample2, 8));

      byte[] sample3 = { -36, -4, 27, 111, 3, 49, 118, -49 };
      assertEquals("dcfc1b6f033176cf", Util.toHexString(sample3, 8));

      byte[] sample4 = { -36, -4, 27, 111, 3, 49, 118, -49, 55};
      assertEquals("dcfc1b6f033176cf", Util.toHexString(sample4, 8));
   }

}
