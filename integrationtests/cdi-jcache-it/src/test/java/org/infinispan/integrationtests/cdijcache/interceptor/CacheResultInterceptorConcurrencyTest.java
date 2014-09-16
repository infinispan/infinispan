package org.infinispan.integrationtests.cdijcache.interceptor;

import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.integrationtests.cdijcache.interceptor.config.Config;
import org.infinispan.integrationtests.cdijcache.interceptor.service.CacheResultService;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.integrationtests.cdijcache.Deployments.baseDeployment;
import static org.testng.Assert.fail;

/**
 * Checks if methods annotated with <code>@CacheResult</code> might be safely accessed with multiple threads.
 *
 * @author Sebastian Laskawiec
 * @see javax.cache.annotation.CacheResult
 */
@Test(groups = "functional", testName = "cdi.test.interceptor.CacheResultInterceptorConcurrencyTest", description = "https://issues.jboss.org/browse/ISPN-4563")
public class CacheResultInterceptorConcurrencyTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CacheResultInterceptorConcurrencyTest.class)
            .addClass(CacheResultService.class)
            .addPackage(Config.class.getPackage())
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class)
            .addAsWebInfResource(MethodHandles.lookup().lookupClass().getResource("/beans-for-not-managed-domain.xml"), "beans.xml");
   }

   @Inject
   private CacheResultService service;

   @Inject
   private BeanManager beanManager;

   private static final int NUMBER_OF_THREADS = 100;

   public void testConcurrentAccessToCache() throws Exception {


      final AtomicReference<Exception> throwableHolder = new AtomicReference<>();
      final CyclicBarrier counter = new CyclicBarrier(NUMBER_OF_THREADS);

      ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
      for(int i = 0; i < NUMBER_OF_THREADS; ++i) {
         executorService.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  counter.await();
                  service.cacheResult("Thread " + Thread.currentThread().getId());
               } catch (Exception t) {
                  throwableHolder.set(t);
               }
            }
         });
      }

      executorService.shutdown();
      if(!executorService.awaitTermination(10, TimeUnit.HOURS)) {
         fail("Executor didn't finish all his tasks!");
      }

      if(throwableHolder.get() != null) {
         throw throwableHolder.get();
      }
   }

}
