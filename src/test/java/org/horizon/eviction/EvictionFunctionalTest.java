package org.horizon.eviction;

import org.horizon.Cache;
import org.horizon.test.TestingUtil;
import org.horizon.config.Configuration;
import org.horizon.config.EvictionConfig;
import org.horizon.container.DataContainer;
import org.horizon.eviction.algorithms.fifo.FIFOAlgorithmConfig;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.util.ReflectionUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "eviction.EvictionFunctionalTest")
public class EvictionFunctionalTest {
   Cache cache;

   @BeforeMethod
   public void setUp() {
      try {
         Configuration cfg = new Configuration();
         EvictionConfig ec = new EvictionConfig();
         cfg.setEvictionConfig(ec);
         FIFOAlgorithmConfig fifo = new FIFOAlgorithmConfig();
         fifo.setMaxEntries(10);
         ec.setAlgorithmConfig(fifo);
         ec.setWakeUpInterval(50); // 50 millis!
         CacheManager cm = new DefaultCacheManager(cfg);
         cache = cm.getCache();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCaches(cache);
   }

   public void testEviction() throws InterruptedException {
      CountDownLatch evictionCompleteLatch = new CountDownLatch(1);
      CountDownLatch cacheFillLatch = new CountDownLatch(1);
      EvictionManager em = cache.getAdvancedCache().getEvictionManager();
      EvictionAlgorithm actualAlgorithm = (EvictionAlgorithm) TestingUtil.extractField(em, "evictionAlgorithm");
      DelegatingAlgorithm a = new DelegatingAlgorithm(actualAlgorithm, cacheFillLatch, evictionCompleteLatch);
      ReflectionUtil.setValue(em, "evictionAlgorithm", a);

      for (int i = 0; i < 20; i++) {
         cache.put(i, "value");
      }
      cacheFillLatch.countDown();

      assert evictionCompleteLatch.await(600, TimeUnit.SECONDS);
      for (int i = 10; i < 20; i++)
         assert cache.get(i).equals("value") : "Key " + i + " should map to 'value', but was " + cache.get(i);

      for (int i = 0; i < 10; i++)
         assert cache.get(i) == null :
               "Key " + i + " should map to null, but was " + cache.get(i) + " and cache count is " + cache.size();
   }

   public class DelegatingAlgorithm implements EvictionAlgorithm {
      EvictionAlgorithm delegate;
      CountDownLatch cacheFillLatch;
      CountDownLatch evictionOverLatch;

      public DelegatingAlgorithm(EvictionAlgorithm delegate, CountDownLatch cacheFillLatch, CountDownLatch evictionOverLatch) {
         this.delegate = delegate;
         this.cacheFillLatch = cacheFillLatch;
         this.evictionOverLatch = evictionOverLatch;
      }

      public void process(BlockingQueue<EvictionEvent> queue) throws EvictionException {
         try {
            cacheFillLatch.await();
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         System.out.println("Queue size=" + queue.size());

         delegate.process(queue);

         // queue should now have processed
         evictionOverLatch.countDown();
      }

      public void resetEvictionQueue() {
         delegate.resetEvictionQueue();
      }

      public void setEvictionAction(EvictionAction evictionAction) {
         delegate.setEvictionAction(evictionAction);
      }

      public void init(Cache<?, ?> cache, DataContainer dataContiner, EvictionAlgorithmConfig evictionAlgorithmConfig) {
         delegate.init(cache, dataContiner, evictionAlgorithmConfig);
      }

      public boolean canIgnoreEvent(EvictionEvent.Type eventType) {
         return delegate.canIgnoreEvent(eventType);
      }

      public Class<? extends EvictionAlgorithmConfig> getConfigurationClass() {
         return delegate.getConfigurationClass();
      }

      public void start() {
         delegate.start();
      }

      public void stop() {
         delegate.stop();
      }
   }
}
