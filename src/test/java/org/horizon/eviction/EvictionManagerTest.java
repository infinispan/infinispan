package org.horizon.eviction;

import org.horizon.config.Configuration;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest {

   private Configuration getCfg() {
      Configuration cfg = new Configuration();
//      EvictionConfig ecfg = new EvictionConfig();
//      ecfg.setAlgorithmConfig(new FIFOAlgorithmConfig()); // for now
//      cfg.setEvictionConfig(ecfg);
      return cfg;
   }

   public void testNoEvictionThread() {
//      EvictionManagerImpl em = new EvictionManagerImpl();
//      Configuration cfg = getCfg();
//      cfg.getEvictionConfig().setWakeUpInterval(0);
//
//      ScheduledExecutorService mockService = EasyMock.createMock(ScheduledExecutorService.class);
//      em.initialize(mockService, cfg, null, null);
//      replay(mockService);
//      em.start();
//
//      assert em.evictionTask == null : "Eviction task is not null!  Should not have scheduled anything!";
//      verify(mockService); // expect that the executor was never used!!
   }

   public void testWakeupInterval() {
//      EvictionManagerImpl em = new EvictionManagerImpl();
//      Configuration cfg = getCfg();
//      cfg.getEvictionConfig().setWakeUpInterval(789);
//
//      ScheduledExecutorService mockService = EasyMock.createMock(ScheduledExecutorService.class);
//      em.initialize(mockService, cfg, null, null);
//
//      ScheduledFuture mockFuture = createNiceMock(ScheduledFuture.class);
//      expect(mockService.scheduleWithFixedDelay(isA(EvictionManagerImpl.ScheduledTask.class), eq((long) 789),
//                                                eq((long) 789), eq(TimeUnit.MILLISECONDS)))
//            .andReturn(mockFuture).once();
//      replay(mockService);
//      em.start();
//
//      assert em.evictionTask == mockFuture;
//      verify(mockService); // expect that the executor was never used!!
   }
}
