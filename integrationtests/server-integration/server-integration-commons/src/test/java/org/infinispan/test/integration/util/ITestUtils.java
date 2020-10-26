package org.infinispan.test.integration.util;

import static org.junit.Assert.assertTrue;

public class ITestUtils {

   public static void sleepForSecs(long numSecs) {
      // give the elements time to be evicted
      try {
         Thread.sleep(numSecs * 1000);
      } catch (InterruptedException e) {
      }
   }

   public static void eventually(Condition ec, long timeout, int loops) {
      if (loops <= 0) {
         throw new IllegalArgumentException("Number of loops must be positive");
      }
      long sleepDuration = timeout / loops;
      if (sleepDuration == 0) {
         sleepDuration = 1;
      }
      try {
         for (int i = 0; i < loops; i++) {

            if (ec.isSatisfied())
               return;
            Thread.sleep(sleepDuration);
         }
         assertTrue(ec.isSatisfied());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   public interface Condition {
      boolean isSatisfied() throws Exception;
   }
}
