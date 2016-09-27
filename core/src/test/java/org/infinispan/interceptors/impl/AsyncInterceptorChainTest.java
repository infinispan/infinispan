package org.infinispan.interceptors.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests {@link AsyncInterceptorChainImpl} concurrent updates
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "functional", testName = "interceptors.AsyncInterceptorChainTest")
public class AsyncInterceptorChainTest {

   private static final Log log = LogFactory.getLog(AsyncInterceptorChainTest.class);

   public void testConcurrentAddRemove() throws Exception {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.emptyList(), AsyncInterceptorChainTest.class.getClassLoader());
      AsyncInterceptorChainImpl ic = new AsyncInterceptorChainImpl(componentMetadataRepo);
      ic.addInterceptor(new DummyCallInterceptor(), 0);
      ic.addInterceptor(new DummyActivationInterceptor(), 1);
      CyclicBarrier barrier = new CyclicBarrier(4);
      List<Future<Void>> futures = new ArrayList<>(2);
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      try {
         // We do test concurrent add/remove of different types per thread,
         // so that the final result is predictable (testable) and that we
         // can not possibly fail because of the InterceptorChain checking
         // that no interceptor is ever added twice.
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new DummyCacheMgmtInterceptor())));
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new DummyDistCacheWriterInterceptor())));
         futures.add(executorService.submit(new InterceptorChainUpdater(ic, barrier, new DummyInvalidationInterceptor())));
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish
         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
      } finally {
         executorService.shutdownNow();
      }
      assert ic.containsInterceptorType(DummyCallInterceptor.class);
      assert ic.containsInterceptorType(DummyActivationInterceptor.class);
      assert ic.containsInterceptorType(DummyCacheMgmtInterceptor.class);
      assert ic.containsInterceptorType(DummyDistCacheWriterInterceptor.class);
      assert ic.containsInterceptorType(DummyInvalidationInterceptor.class);
      assert ic.getInterceptors().size() == 5 : "Resulting interceptor chain was actually " + ic.getInterceptors();
   }

   private static class InterceptorChainUpdater implements Callable<Void> {
      private final AsyncInterceptorChain ic;
      private final CyclicBarrier barrier;
      private final AsyncInterceptor interceptor;

      InterceptorChainUpdater(AsyncInterceptorChain ic, CyclicBarrier barrier, AsyncInterceptor interceptor) {
         this.ic = ic;
         this.barrier = barrier;
         this.interceptor = interceptor;
      }

      @Override
      public Void call() throws Exception {
         final Class<? extends AsyncInterceptor> interceptorClass = interceptor.getClass();
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

   private static class DummyCallInterceptor extends BaseAsyncInterceptor {
      @Override
      public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command)
            throws Throwable {
         return null;
      }
   }

   private static class DummyActivationInterceptor extends DummyCallInterceptor {
   }

   private static class DummyCacheMgmtInterceptor extends DummyCallInterceptor {
   }

   private static class DummyDistCacheWriterInterceptor extends DummyCallInterceptor {
   }

   private static class DummyInvalidationInterceptor extends DummyCallInterceptor {
   }

}
