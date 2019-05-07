package org.infinispan.server.hotrod;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.iteration.IterationManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(testName = "server.hotrod.HotRodIteratorReapTest", groups = "functional")
@CleanupAfterMethod
public class HotRodIteratorReapTest extends HotRodSingleNodeTest {
   ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = super.createCacheManager();
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      return cm;
   }

   public void testIterationStateReaperOnClosedConnections() throws InterruptedException {
      IterationManager iterationManager = server().getIterationManager();
      for (int i = 0; i < 10; i++) {
         hotRodClient.iteratorStart(null, null, null, 10, false);
      }
      assertEquals(10, iterationManager.activeIterations());
      hotRodClient.stop().await();
      assertEquals(0, iterationManager.activeIterations());
   }

   public void testIterationStateReaperOnTimeout() {
      IterationManager iterationManager = server().getIterationManager();
      for (int i = 0; i < 10; i++) {
         hotRodClient.iteratorStart(null, null, null, 10, false);
      }
      assertEquals(10, iterationManager.activeIterations());
      timeService.advance(TimeUnit.MINUTES.toMillis(5));
      assertEquals(0, iterationManager.activeIterations());
   }


}
