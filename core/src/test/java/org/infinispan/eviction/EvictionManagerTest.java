package org.infinispan.eviction;

import static org.easymock.EasyMock.*;
import org.easymock.IAnswer;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalEntryFactory;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "eviction.EvictionManagerTest")
public class EvictionManagerTest {

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

   public void testProcessingEviction() {
      EvictionManagerImpl em = new EvictionManagerImpl();
      Configuration cfg = getCfg();
      cfg.setEvictionWakeUpInterval(0);
      cfg.setEvictionMaxEntries(100);

      ScheduledExecutorService mockService = createMock(ScheduledExecutorService.class);
      DataContainer mockDataContainer = createMock(DataContainer.class);
      Cache mockCache = createMock(Cache.class);
      em.initialize(mockService, cfg, mockCache, mockDataContainer, null);
      replay(mockService);
      em.start();
      verify(mockService); // expect that the executor was never used!!

      // now manually process stuff.
      reset(mockDataContainer, mockCache);
      mockDataContainer.purgeExpired();
      expectLastCall().once();
      SizeGenerator sg = new SizeGenerator(500, 500, 1000, 101, 100, 99);
      expect(mockDataContainer.size()).andAnswer(sg).anyTimes();
      Iterator mockIterator = createMock(Iterator.class);
      expect(mockIterator.hasNext()).andReturn(true).anyTimes();
      expect(mockIterator.next()).andReturn(InternalEntryFactory.create("key", "value")).anyTimes();
      expect(mockDataContainer.iterator()).andReturn(mockIterator).once();
      mockCache.evict(eq("key"));
      expectLastCall().times(3);

      replay(mockDataContainer, mockIterator, mockCache);
      em.processEviction();
      verify(mockDataContainer, mockIterator, mockCache);
   }

   private static class SizeGenerator implements IAnswer<Integer> {
      int[] sizesToReport;
      int idx = 0;

      public SizeGenerator(int... sizesToReport) {
         this.sizesToReport = sizesToReport;
      }

      public Integer answer() throws Throwable {
         return sizesToReport[idx++];
      }
   }
}
