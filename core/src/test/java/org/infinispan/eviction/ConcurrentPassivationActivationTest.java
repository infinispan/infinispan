package org.infinispan.eviction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests behaviour when concurrent passivation/activation operations occur.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "eviction.ConcurrentPassivationActivationTest")
public class ConcurrentPassivationActivationTest extends SingleCacheManagerTest {

   final CountDownLatch passivateWait = new CountDownLatch(1);
   final CountDownLatch activationWait = new CountDownLatch(1);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().strategy(EvictionStrategy.LRU).maxEntries(1)
         .jmxStatistics().enable()
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testInMemoryEntryNotLostWithConcurrentActivePassive() throws Exception {
      ActivationManager activation =
            TestingUtil.extractComponent(cache, ActivationManager.class);

      PassivationManager passivation =
            TestingUtil.extractComponent(cache, PassivationManager.class);

      cache.addListener(new SlowPassivator());

      assertEquals(0, activation.getActivationCount());
      assertEquals(0, passivation.getPassivationCount());

      // 1. Store an entry in the cache
      cache.put(1, "v1");
      assertEquals(0, activation.getActivationCount());
      assertEquals(0, passivation.getPassivationCount());

      // 2. Add another entry and block just after passivation has stored
      // entry in cache store, but it's still in memory
      Future<Object> passivatorFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            // Store a second entry to force the previous entry
            // to be evicted and passivated
            log.debug("Store another entry and force previous to be passivated");
            cache.put(2, "v2");
            return null;
         }
      });

      // 3. Retrieve entry to be passivated from memory
      // and let it remove it from cache store
      Future<Object> activatorFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            // Retrieve first key forcing an activation
            log.debug("Retrieve entry and force activation");
            activationWait.await(60, TimeUnit.SECONDS);
            assertEquals("v1", cache.get(1));
            return null;
         }
      });

      activatorFuture.get(30, TimeUnit.SECONDS);
      assertEquals(0, activation.getActivationCount());
      assertEquals(1, passivation.getPassivationCount());

      // 4. With entry stored, then removed, now let the passivator thread
      // remove the entry from memory
      passivateWait.countDown();

      // 5. Wait for entry to be removed from memory...
      passivatorFuture.get(30, TimeUnit.SECONDS);

      // 6. Entry shouldn't be gone
      assertEquals("v1", cache.get(1));
      assertEquals(1, activation.getActivationCount());
      // Second key gets passivated now to make space for the 1st one
      assertEquals(2, passivation.getPassivationCount());
   }

   @Listener
   public class SlowPassivator {

      @CacheEntryPassivated
      @SuppressWarnings("unused")
      public void passivate(CacheEntryPassivatedEvent event) throws Exception {
         if (!event.isPre()) {
            log.debugf("Entry stored in cache store, wait before removing from memory");
            activationWait.countDown();
            passivateWait.await(60, TimeUnit.SECONDS);
         }
      }

   }

}
