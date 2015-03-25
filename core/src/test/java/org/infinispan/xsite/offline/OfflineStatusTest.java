package org.infinispan.xsite.offline;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.xsite.OfflineStatus;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite, functional", testName = "xsite.offline.OfflineStatusTest")
public class OfflineStatusTest extends AbstractInfinispanTest {

   public void timeBasedTakeOffline() {
      final OfflineStatus offlineStatus = new OfflineStatus(new TakeOfflineConfigurationBuilder(null, null).afterFailures(10).minTimeToWait(3000).create(), TIME_SERVICE);

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
      final OfflineStatus offlineStatus = new OfflineStatus(new TakeOfflineConfigurationBuilder(null, null).afterFailures(10).minTimeToWait(0).create(), TIME_SERVICE);
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
