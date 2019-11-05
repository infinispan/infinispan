package org.infinispan.reactive;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

@Test(groups = "functional", testName = "reactive.BlockHoundTest")
public class BlockHoundTest extends AbstractInfinispanTest {
   @Test
   public void testBlocking() {
      withCacheManager(TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false), ecm -> {
         Cache cache = ecm.getCache();
         Scheduler cpuScheduler = Schedulers.from(TestingUtil.extractGlobalComponent(ecm,
               ExecutorService.class, KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR));
         try {
            Flowable.just(1)
                  .subscribeOn(cpuScheduler)
                  .doOnNext(it -> cache.size() )
                  .blockingSubscribe();
            fail("Expected it to fail!");
         } catch (CacheException e) {
            assertTrue(e.getMessage().contains("Blocking call!"));
         }
      });
   }
}
