package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.TransactionSuspendedCacheNotifierTest")
@CleanupAfterMethod
public class TransactionSuspendedCacheNotifierTest extends SingleCacheManagerTest {

   private InvocationContext context;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      context = new NonTxInvocationContext(AnyEquivalence.getInstance());
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testTransactionSuspended() throws Exception {
      //to avoid stack overflow
      CacheNotifierImpl notifier = new CacheNotifierImpl();
      notifier.injectDependencies(cache, cache.getAdvancedCache().getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class),
                                  cache.getAdvancedCache().getTransactionManager());
      notifier.addListener(new TestListener(log), null, null);
      notifier.start(); //sets the sync executor

      assertTrue(cache.isEmpty());
      tm().begin();
      //makes no sense to test all the even types since they all go to the same code path.
      notifier.notifyCacheEntryActivated(null, null, false, context, null);
      tm().rollback();

      //if the transaction is not suspended, the handle method will be attached to the transaction a put opertation.
      //then, the rollback will not write anything in the cache
      assertFalse(cache.isEmpty());
      assertEquals(1, cache.get(Event.Type.CACHE_ENTRY_ACTIVATED));
   }

   @Listener(sync = true)
   public static class TestListener {

      private final Log log;

      public TestListener(Log log) {
         this.log = log;
      }

      @CacheEntryActivated
      public void handle(Event e) {
         log.debugf("Event triggered! %s", e);
         Cache cache = e.getCache();
         Integer count = (Integer) cache.get(e.getType());
         count = count == null ? 1 : count + 1;
         cache.put(e.getType(), count);
         log.debugf("Event triggered! %s. Added to cache: %s", e, count);
      }

   }
}
