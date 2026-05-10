package org.infinispan.util;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.EmbeddedTimeServiceTest")
public class EmbeddedTimeServiceTest {

   public void testMonotonicIncrement() {
      TimeService timeService = TIME_SERVICE;
      //less or equals in all the cases because the system may not have enough precision and the methods may return
      // the same value.
      assertTrue(timeService.time() <= timeService.time());
      assertTrue(timeService.wallClockTime() <= timeService.wallClockTime());
   }

   public void testDuration() {
      TimeService timeService = new EmbeddedTimeService() {
         @Override
         public long time() {
            return 10;
         }

         @Override
         public long wallClockTime() {
            return 10;
         }
      };
      assertEquals(10, timeService.timeDuration(0, TimeUnit.NANOSECONDS));
      assertEquals(11, timeService.timeDuration(-1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(10, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(11, TimeUnit.NANOSECONDS));
      assertEquals(1, timeService.timeDuration(9, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(9, TimeUnit.MICROSECONDS));
      assertEquals(0, timeService.timeDuration(9, TimeUnit.MILLISECONDS));

      assertEquals(1, timeService.timeDuration(0, 1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(0, -1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(1, 0, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(1, -1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(-1, -1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.timeDuration(0, 0, TimeUnit.NANOSECONDS));
      assertEquals(1, timeService.timeDuration(0, 1000, TimeUnit.MICROSECONDS));
      assertEquals(1, timeService.timeDuration(0, 1000000, TimeUnit.MILLISECONDS));
   }

   public void testExpired() {
      TimeService timeService = new EmbeddedTimeService() {
         @Override
         public long time() {
            return 10;
         }
      };

      assertTrue(timeService.isTimeExpired(-1));
      assertTrue(timeService.isTimeExpired(0));
      assertTrue(timeService.isTimeExpired(9));
      assertTrue(timeService.isTimeExpired(10));
      assertFalse(timeService.isTimeExpired(11));
   }

   public void testRemainingTime() {
      TimeService timeService = new EmbeddedTimeService() {
         @Override
         public long time() {
            return 10;
         }
      };

      assertEquals(0, timeService.remainingTime(-1, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.remainingTime(0, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.remainingTime(9, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.remainingTime(10, TimeUnit.NANOSECONDS));
      assertEquals(1, timeService.remainingTime(11, TimeUnit.NANOSECONDS));
      assertEquals(0, timeService.remainingTime(11, TimeUnit.MICROSECONDS));
      assertEquals(0, timeService.remainingTime(11, TimeUnit.MILLISECONDS));
   }

   public void testExpectedTime() {
      TimeService timeService = new EmbeddedTimeService() {
         @Override
         public long time() {
            return 10;
         }

         @Override
         public long wallClockTime() {
            return 10;
         }
      };

      assertEquals(10, timeService.expectedEndTime(-1, TimeUnit.NANOSECONDS));
      assertEquals(10, timeService.expectedEndTime(0, TimeUnit.NANOSECONDS));
      assertEquals(11, timeService.expectedEndTime(1, TimeUnit.NANOSECONDS));
      assertEquals(19, timeService.expectedEndTime(9, TimeUnit.NANOSECONDS));
      assertEquals(20, timeService.expectedEndTime(10, TimeUnit.NANOSECONDS));
      assertEquals(21, timeService.expectedEndTime(11, TimeUnit.NANOSECONDS));
      assertEquals(11010, timeService.expectedEndTime(11, TimeUnit.MICROSECONDS));
      assertEquals(11000010, timeService.expectedEndTime(11, TimeUnit.MILLISECONDS));
   }

}
