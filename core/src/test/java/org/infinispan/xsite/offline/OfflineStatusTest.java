/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.xsite.offline;

import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.xsite.OfflineStatus;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite, functional", testName = "xsite.offline.OfflineStatusTest")
public class OfflineStatusTest extends AbstractInfinispanTest {

   public void timeBasedTakeOffline() {
      final OfflineStatus offlineStatus = new OfflineStatus(new TakeOfflineConfiguration(10, 3000), TIME_SERVICE);

      assert !offlineStatus.isOffline();
      for (int i = 0; i < 9; i++) {
         offlineStatus.updateOnCommunicationFailure(now());
      }

      assertEquals(9, offlineStatus.getFailureCount());
      assert !offlineStatus.isOffline();
      assert !offlineStatus.minTimeHasElapsed() : offlineStatus.millisSinceFirstFailure();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            Thread.sleep(1000);
            return offlineStatus.minTimeHasElapsed();
         }
      });

      assertEquals(9, offlineStatus.getFailureCount());
      assert !offlineStatus.isOffline();
      assert offlineStatus.minTimeHasElapsed();


      offlineStatus.updateOnCommunicationFailure(now());
      assertEquals(10, offlineStatus.getFailureCount());
      assert offlineStatus.isOffline();
      assert offlineStatus.minTimeHasElapsed();
   }

   public void testFailureBasedOnly() throws Throwable {
      final OfflineStatus offlineStatus = new OfflineStatus(new TakeOfflineConfiguration(10, 0), TIME_SERVICE);
      test(offlineStatus);
      offlineStatus.reset();
      test(offlineStatus);
   }

   private void test(OfflineStatus offlineStatus) throws InterruptedException {
      for (int i = 0; i < 9; i++) {
         offlineStatus.updateOnCommunicationFailure(now());
      }
      assert !offlineStatus.isOffline();
      Thread.sleep(2000);
      assert !offlineStatus.isOffline();

      offlineStatus.updateOnCommunicationFailure(now());
      assertEquals(10, offlineStatus.getFailureCount());
      assert offlineStatus.isOffline();
   }

   private long now() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
   }
}
