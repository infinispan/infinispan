/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util;

import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;
import static org.testng.Assert.*;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.DefaultTimeServiceTest")
public class DefaultTimeServiceTest {

   public void testMonotonicIncrement() {
      TimeService timeService = TIME_SERVICE;
      assertTrue(timeService.time() < timeService.time());
      assertTrue(timeService.time() <= timeService.time());

      //<= in all the cases because the operation duration is less the 1 millisecond
      assertTrue(timeService.wallClockTime() <= timeService.wallClockTime());
      assertTrue(timeService.wallClockTime() <= timeService.wallClockTime());
   }

   public void testDuration() {
      TimeService timeService = new DefaultTimeService() {
         @Override
         public long time() {
            return 10;
         }

         @Override
         public long wallClockTime() {
            return 10;
         }
      };
      assertEquals(timeService.timeDuration(0, TimeUnit.NANOSECONDS), 10);
      assertEquals(timeService.timeDuration(-1, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(10, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(11, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(9, TimeUnit.NANOSECONDS), 1);
      assertEquals(timeService.timeDuration(9, TimeUnit.MICROSECONDS), 0);
      assertEquals(timeService.timeDuration(9, TimeUnit.MILLISECONDS), 0);

      assertEquals(timeService.timeDuration(0, 1, TimeUnit.NANOSECONDS), 1);
      assertEquals(timeService.timeDuration(0, -1, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(1, 0, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(1, -1, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(-1, -1, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(0, 0, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.timeDuration(0, 1000, TimeUnit.MICROSECONDS), 1);
      assertEquals(timeService.timeDuration(0, 1000000, TimeUnit.MILLISECONDS), 1);
   }

   public void testExpired() {
      TimeService timeService = new DefaultTimeService() {
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
      TimeService timeService = new DefaultTimeService() {
         @Override
         public long time() {
            return 10;
         }
      };

      assertEquals(timeService.remainingTime(-1, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.remainingTime(0, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.remainingTime(9, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.remainingTime(10, TimeUnit.NANOSECONDS), 0);
      assertEquals(timeService.remainingTime(11, TimeUnit.NANOSECONDS), 1);
      assertEquals(timeService.remainingTime(11, TimeUnit.MICROSECONDS), 0);
      assertEquals(timeService.remainingTime(11, TimeUnit.MILLISECONDS), 0);
   }

   public void testExpectedTime() {
      TimeService timeService = new DefaultTimeService() {
         @Override
         public long time() {
            return 10;
         }

         @Override
         public long wallClockTime() {
            return 10;
         }
      };

      assertEquals(timeService.expectedEndTime(-1, TimeUnit.NANOSECONDS), 10);
      assertEquals(timeService.expectedEndTime(0, TimeUnit.NANOSECONDS), 10);
      assertEquals(timeService.expectedEndTime(1, TimeUnit.NANOSECONDS), 11);
      assertEquals(timeService.expectedEndTime(9, TimeUnit.NANOSECONDS), 19);
      assertEquals(timeService.expectedEndTime(10, TimeUnit.NANOSECONDS), 20);
      assertEquals(timeService.expectedEndTime(11, TimeUnit.NANOSECONDS), 21);
      assertEquals(timeService.expectedEndTime(11, TimeUnit.MICROSECONDS), 11010);
      assertEquals(timeService.expectedEndTime(11, TimeUnit.MILLISECONDS), 11000010);
   }

}
