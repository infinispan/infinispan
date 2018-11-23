package org.infinispan.client.hotrod.impl;

import static org.testng.Assert.assertTrue;

import org.infinispan.commons.time.ControlledTimeService;
import org.testng.annotations.Test;

/**
 * @since 10.0
 * @author Diego Lovison
 */
@Test(groups = "functional", testName = "client.hotrod.impl.ClientStatisticsTest")
public class ClientStatisticsTest {

   public void testAverageRemoteStoreTime() {

      ControlledTimeService timeService = new ControlledTimeService();
      ClientStatistics clientStatistics = new ClientStatistics(true, timeService);
      // given: a put operation
      long now = timeService.time();
      // when: network is slow
      timeService.advance(1200);
      clientStatistics.dataStore(now, 1);
      // then:
      assertTrue(clientStatistics.getAverageRemoteStoreTime() > 0);
   }

   public void testAverageRemoteReadTime() {

      ControlledTimeService timeService = new ControlledTimeService();
      ClientStatistics clientStatistics = new ClientStatistics(true, timeService);
      // given: a get operation
      long now = timeService.time();
      // when: network is slow
      timeService.advance(1200);
      clientStatistics.dataRead(true, now, 1);
      // then:
      assertTrue(clientStatistics.getAverageRemoteReadTime() > 0);
   }

   public void testAverageRemovesTime() {

      ControlledTimeService timeService = new ControlledTimeService();
      ClientStatistics clientStatistics = new ClientStatistics(true, timeService);
      // given: a remove operation
      long now = timeService.time();
      // when: network is slow
      timeService.advance(1200);
      clientStatistics.dataRemove(now, 1);
      // then:
      assertTrue(clientStatistics.getAverageRemoteRemovesTime() > 0);
   }
}
