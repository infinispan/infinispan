package org.infinispan.commons.test.skip;


import java.util.Arrays;

import org.testng.SkipException;

/**
 * Allows to skip a test on certain Operation Systems.
 */
public class SkipTestNG {
   /**
    * Use within a {@code @Test} method to skip that method on some OSes.
    * Use in a {@code @BeforeClass} method to skip all methods in a class on some OSes.
    */
   public static void skipOnOS(OS... oses) {
      OS currentOs = OS.getCurrentOs();
      if (Arrays.asList(oses).contains(currentOs)) {
         throw new SkipException("Skipping test on " + currentOs);
      }
   }
}
