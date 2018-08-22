package org.infinispan.expiry;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiry.AutoCommitExpiryTest")
public abstract class AutoCommitExpiryTest extends SingleCacheManagerTest {
   private final CacheMode mode;
   private final boolean autoCommit;

   protected ControlledTimeService timeService;

   protected AutoCommitExpiryTest(CacheMode mode, boolean autoCommit) {
      this.mode = mode;
      this.autoCommit = autoCommit;
   }

   @Test
   public void testNoAutCommitAndExpiryListener() throws SystemException, NotSupportedException,
         HeuristicRollbackException, HeuristicMixedException, RollbackException {
      ExpiryListener expiryListener = new ExpiryListener();

      Cache<String, String> applicationCache = cacheManager.getCache();
      applicationCache.addListener(expiryListener);

      TransactionManager tm = applicationCache.getAdvancedCache().getTransactionManager();
      tm.begin();
      applicationCache.put("test1", "value1", 1, TimeUnit.SECONDS);
      tm.commit();

      tm.begin();
      applicationCache.put("test2", "value2", 1, TimeUnit.SECONDS);
      tm.commit();

      timeService.advance(TimeUnit.SECONDS.toMillis(10));

      ExpirationManager manager = applicationCache.getAdvancedCache().getExpirationManager();
      manager.processExpiration();

      assertEquals(2, expiryListener.getCount());
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (mode.isClustered()) {
         builder.clustering().cacheMode(mode);
      }
      builder
            .jmxStatistics().enable()
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .autoCommit(autoCommit)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder);

      timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      return cm;
   }

   @Listener(primaryOnly = true, observation = Listener.Observation.POST)
   public class ExpiryListener {

      private final AtomicInteger counter = new AtomicInteger();

      public int getCount() {
         return counter.get();
      }

      @CacheEntryExpired
      public void expired(CacheEntryExpiredEvent<String, String> event) {
         counter.incrementAndGet();
      }
   }
}
