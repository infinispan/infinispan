package org.infinispan.interceptors;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.interceptors.base.AnyInterceptor;
import org.infinispan.interceptors.sequential.InvalidationInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests {@link SequentialInterceptorChain} concurrent updates
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.1
 */
@Test(groups = "functional", testName = "interceptors.SequentialInterceptorChainTest")
public class SequentialInterceptorChainTest {

   private static final Log log = LogFactory.getLog(SequentialInterceptorChainTest.class);

   public void testConcurrentAddRemove() throws Exception {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.<ModuleMetadataFileFinder>emptyList(), SequentialInterceptorChainTest.class.getClassLoader());
      SequentialInterceptorChainImpl ic = new SequentialInterceptorChainImpl(componentMetadataRepo);
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();
      ic.inject(null, globalConfiguration);
      ic.addInterceptor(new org.infinispan.interceptors.sequential.CallInterceptor(), 0);
      ic.addInterceptor(new ActivationInterceptor(), 1);
      CyclicBarrier barrier = new CyclicBarrier(4);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(2);
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      try {
         // We do test concurrent add/remove of different types per thread,
         // so that the final result is predictable (testable) and that we
         // can not possibly fail because of the InterceptorChain checking
         // that no interceptor is ever added twice.
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new CacheMgmtInterceptor())));
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new DistCacheWriterInterceptor())));
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new InvalidationInterceptor())));
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish
         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
      } finally {
         executorService.shutdownNow();
      }
      assert ic.containsInterceptorType(org.infinispan.interceptors.sequential.CallInterceptor.class);
      assert ic.containsInterceptorType(ActivationInterceptor.class);
      assert ic.containsInterceptorType(CacheMgmtInterceptor.class);
      assert ic.containsInterceptorType(DistCacheWriterInterceptor.class);
      assert ic.containsInterceptorType(org.infinispan.interceptors.sequential.InvalidationInterceptor.class);
      assert ic.getInterceptors().size() == 5 : "Resulting interceptor chain was actually " + ic.getInterceptors();
   }

   private static class InterceptorChainUpdater implements Callable<Void> {
      private final SequentialInterceptorChain ic;
      private final CyclicBarrier barrier;
      private final AnyInterceptor interceptor;

      InterceptorChainUpdater(SequentialInterceptorChain ic, CyclicBarrier barrier, AnyInterceptor interceptor) {
         this.ic = ic;
         this.barrier = barrier;
         this.interceptor = interceptor;
      }

      @Override
      public Void call() throws Exception {
         final Class<? extends AnyInterceptor> interceptorClass = interceptor.getClass();
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();
            // test in a loop as the barrier is otherwise not enough to make sure
            // the different testing threads actually do make changes concurrently
            // 2000 is still almost nothing in terms of testsuite time.
            for (int i = 0; i < 2000; i++) {
               ic.removeInterceptor(interceptorClass);
               ic.addInterceptor(interceptor, 1);
            }
            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }

}
