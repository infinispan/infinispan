package org.infinispan.xsite.offline;

import static java.lang.String.format;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.notification.SiteStatusListener;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.offline.OfflineStatusTest")
public class OfflineStatusTest extends AbstractInfinispanTest {

   public void timeBasedTakeOffline() {
      //Tests if the offline status changes when the minimum time has elapsed and after failure counter.
      final long minTimeWait = 3000;
      final int minFailures = 10;
      final TestContext context = createNew(minTimeWait, minFailures);

      //first part, we reached the min failures but not the min time.
      for (int i = 0; i < minFailures + 1; i++) {
         assertOffline(context, false);
         addCommunicationFailure(context);
      }

      assertMinFailureCount(context, minFailures + 1);
      assertMinTimeElapsed(context, false);
      assertOffline(context, false);

      context.timeService.advance(minTimeWait + 1);

      assertMinTimeElapsed(context, true);
      assertMinFailureCount(context, minFailures + 1);
      assertOffline(context, true);

      context.offlineStatus.reset(); //reset everything

      //second part, we reached the min time, but not the failures-
      for (int i = 0; i < minFailures -1; i++) {
         assertOffline(context, false);
         addCommunicationFailure(context);
      }

      assertMinFailureCount(context, minFailures -1);
      assertMinTimeElapsed(context, false);
      assertOffline(context, false);

      context.timeService.advance(minTimeWait + 1);

      assertMinFailureCount(context, minFailures -1);
      assertMinTimeElapsed(context, true);
      assertOffline(context, false);

      addCommunicationFailure(context);

      assertMinTimeElapsed(context, true);
      assertMinFailureCount(context, minFailures);
      assertOffline(context, true);

      context.listener.check(SiteStatus.OFFLINE, SiteStatus.ONLINE, SiteStatus.OFFLINE);
   }

   public void testFailureBasedOnly() throws Throwable {
      //note: can't check the min time since it throws an IllegalStateException when disabled
      final int minFailures = 10;
      final TestContext context = createNew(0, minFailures);

      for (int i = 0; i < minFailures - 1; i++) {
         assertOffline(context, false);
         addCommunicationFailure(context);
      }

      assertMinFailureCount(context, minFailures - 1);
      assertOffline(context, false);

      context.timeService.advance(1);

      assertMinFailureCount(context, minFailures - 1);
      assertOffline(context, false);

      addCommunicationFailure(context);

      assertMinFailureCount(context, minFailures);
      assertOffline(context, true);

      context.listener.check(SiteStatus.OFFLINE);
   }

   public void testTimeBasedOnly() throws Throwable {
      final long minWaitTime = 3000;
      final int minFailures = 10;
      final TestContext context = createNew(minWaitTime, -1);

      for (int i = 0; i < minFailures; i++) {
         assertOffline(context, false);
         addCommunicationFailure(context);
      }

      assertMinFailureCount(context, minFailures);
      assertMinTimeElapsed(context, false);
      assertOffline(context, false);

      context.timeService.advance(minWaitTime + 1);

      assertMinFailureCount(context, minFailures);
      assertMinTimeElapsed(context, true);
      assertOffline(context, true);

      context.listener.check(SiteStatus.OFFLINE);
   }

   public void testForceOffline() {
      //note: can't check the min time since it throws an IllegalStateException when disabled
      final TestContext context = createNew(-1, -1);

      addCommunicationFailure(context);
      context.timeService.advance(1);

      assertMinFailureCount(context, 1);
      assertOffline(context, false);

      context.offlineStatus.forceOffline();

      assertMinFailureCount(context, 1);
      assertOffline(context, true);

      //test bring online
      context.offlineStatus.bringOnline();

      assertMinFailureCount(context, 0);
      assertOffline(context, false);

      addCommunicationFailure(context);
      context.timeService.advance(1);

      assertMinFailureCount(context, 1);
      assertOffline(context, false);

      context.offlineStatus.forceOffline();

      assertMinFailureCount(context, 1);
      assertOffline(context, true);

      //test reset
      context.offlineStatus.reset();

      assertMinFailureCount(context, 0);
      assertOffline(context, false);

      context.listener.check(SiteStatus.OFFLINE, SiteStatus.ONLINE, SiteStatus.OFFLINE, SiteStatus.ONLINE);
   }

   private static void assertOffline(TestContext context, boolean expected) {
      assertEquals("Checking offline.", expected, context.offlineStatus.isOffline());
   }

   private static void assertMinFailureCount(TestContext context, int expected) {
      assertEquals("Check failure count.", expected, context.offlineStatus.getFailureCount());
   }

   private static void assertMinTimeElapsed(TestContext context, boolean expected) {
      assertEquals(format("Check min time has elapsed. Current time=%d. Time elapsed=%d", context.timeService.time(),
                                      context.offlineStatus.millisSinceFirstFailure()),
                               expected,
                               context.offlineStatus.minTimeHasElapsed());

   }

   private static void addCommunicationFailure(TestContext context) {
      context.offlineStatus.updateOnCommunicationFailure(context.timeService.wallClockTime());
   }

   private static TestContext createNew(long minWait, int afterFailures) {
      ControlledTimeService t = new ControlledTimeService();
      ListenerImpl l = new ListenerImpl();
      TakeOfflineConfiguration c = new TakeOfflineConfigurationBuilder(null, null)
            .afterFailures(afterFailures)
            .minTimeToWait(minWait)
            .create();
      return new TestContext(new OfflineStatus(c, t, l), t, l);
   }

   private static class TestContext {
      private final OfflineStatus offlineStatus;
      private final ControlledTimeService timeService;
      private final ListenerImpl listener;

      private TestContext(OfflineStatus offlineStatus, ControlledTimeService timeService, ListenerImpl listener) {
         this.offlineStatus = offlineStatus;
         this.timeService = timeService;
         this.listener = listener;
      }
   }

   private static class ListenerImpl implements SiteStatusListener {
      private final Queue<SiteStatus> notifications;

      private ListenerImpl() {
         notifications = new ConcurrentLinkedDeque<>();
      }

      @Override
      public void siteOnline() {
         notifications.add(SiteStatus.ONLINE);
      }

      @Override
      public void siteOffline() {
         notifications.add(SiteStatus.OFFLINE);
      }

      private void check(SiteStatus first) {
         assertEquals("Check first site status.",  first, notifications.poll());
         assertTrue("Check notifications is empty.", notifications.isEmpty());
      }

      private void check(SiteStatus first, SiteStatus... remaining) {
         assertEquals("Check first site status.",  first, notifications.poll());
         int i = 2;
         for (SiteStatus status : remaining) {
            assertEquals(format("Check %d(\"th\") site status", i), status, notifications.poll());
            i++;
         }
         assertTrue("Check notifications is empty.", notifications.isEmpty());
      }
   }

   private enum SiteStatus {
      ONLINE, OFFLINE
   }
}
