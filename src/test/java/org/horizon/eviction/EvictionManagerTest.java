package org.horizon.eviction;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.horizon.config.Configuration;
import org.horizon.config.EvictionConfig;
import org.horizon.eviction.algorithms.fifo.FIFOAlgorithmConfig;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.eviction.events.InUseEvictionEvent;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest {

   private Configuration getCfg() {
      Configuration cfg = new Configuration();
      EvictionConfig ecfg = new EvictionConfig();
      ecfg.setAlgorithmConfig(new FIFOAlgorithmConfig()); // for now
      cfg.setEvictionConfig(ecfg);
      return cfg;
   }

   public void testNoEvictionThread() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg();
      cfg.getEvictionConfig().setWakeUpInterval(0);

      ScheduledExecutorService mockService = EasyMock.createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null);
      replay(mockService);
      em.start();

      assert em.evictionTask == null : "Eviction task is not null!  Should not have scheduled anything!";
      verify(mockService); // expect that the executor was never used!!
   }

   public void testWakeupInterval() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg();
      cfg.getEvictionConfig().setWakeUpInterval(789);

      ScheduledExecutorService mockService = EasyMock.createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null);

      ScheduledFuture mockFuture = createNiceMock(ScheduledFuture.class);
      expect(mockService.scheduleWithFixedDelay(isA(EvictionManagerImpl.ScheduledTask.class), eq((long) 789),
                                                eq((long) 789), eq(TimeUnit.MILLISECONDS)))
            .andReturn(mockFuture).once();
      replay(mockService);
      em.start();

      assert em.evictionTask == mockFuture;
      verify(mockService); // expect that the executor was never used!!
   }

   public void testMarkInUse() throws InterruptedException {
      EvictionManagerImpl em = new EvictionManagerImpl();
      em.evictionEventQueue = createMock(BlockingQueue.class);
      em.evictionAlgorithm = createNiceMock(EvictionAlgorithm.class);
      expect(em.evictionEventQueue.size()).andReturn(0).anyTimes();
      em.evictionEventQueue.put(eq(new InUseEvictionEvent("x", 7000)));
      expectLastCall().once();
      em.evictionEventQueue.put(eq(new EvictionEvent("x", EvictionEvent.Type.UNMARK_IN_USE_EVENT)));
      expectLastCall().once();

      replay(em.evictionEventQueue);
      em.markKeyCurrentlyInUse("x", 7, TimeUnit.SECONDS);
      em.unmarkKeyCurrentlyInUse("x");
      verify(em.evictionEventQueue);
   }
}
