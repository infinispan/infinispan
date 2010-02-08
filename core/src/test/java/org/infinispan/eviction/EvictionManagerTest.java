package org.infinispan.eviction;

import static org.easymock.EasyMock.*;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest extends AbstractInfinispanTest {

   private Configuration getCfg() {
      Configuration cfg = new Configuration();
      cfg.setEvictionStrategy(EvictionStrategy.FIFO);
      return cfg;
   }

   public void testNoEvictionThread() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg();
      cfg.setEvictionWakeUpInterval(0);

      ScheduledExecutorService mockService = createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null, null);
      replay(mockService);
      em.start();

      assert em.evictionTask == null : "Eviction task is not null!  Should not have scheduled anything!";
      verify(mockService); // expect that the executor was never used!!
   }

   public void testWakeupInterval() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg();
      cfg.setEvictionWakeUpInterval(789);

      ScheduledExecutorService mockService = createMock(ScheduledExecutorService.class);
      em.initialize(mockService, cfg, null, null, null);

      ScheduledFuture mockFuture = createNiceMock(ScheduledFuture.class);
      expect(mockService.scheduleWithFixedDelay(isA(EvictionManagerImpl.ScheduledTask.class), eq((long) 789),
                                                eq((long) 789), eq(TimeUnit.MILLISECONDS)))
            .andReturn(mockFuture).once();
      replay(mockService);
      em.start();

      assert em.evictionTask == mockFuture;
      verify(mockService); // expect that the executor was never used!!
   }
}
