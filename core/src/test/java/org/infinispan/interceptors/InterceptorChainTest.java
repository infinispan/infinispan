package org.infinispan.interceptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests {@link InterceptorChain} logic
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.1
 */
@Test(groups = "functional", testName = "interceptors.InterceptorChainTest")
public class InterceptorChainTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(InterceptorChainTest.class);

   public void testConcurrentAddRemove() throws Exception {
      ComponentMetadataRepo componentMetadataRepo = new ComponentMetadataRepo();
      componentMetadataRepo.initialize(Collections.<ModuleMetadataFileFinder>emptyList(), InterceptorChainTest.class.getClassLoader());
      InterceptorChain ic = new InterceptorChain(componentMetadataRepo);
      ic.setFirstInChain(new CallInterceptor());
      ic.addInterceptor(new ActivationInterceptor(), 1);
      CyclicBarrier barrier = new CyclicBarrier(4);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(2);
      // We do test concurrent add/remove of different types per thread,
      // so that the final result is predictable (testable) and that we
      // can not possibly fail because of the InterceptorChain checking
      // that no interceptor is ever added twice.
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new CacheMgmtInterceptor())));
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new DistCacheWriterInterceptor())));
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new InvalidationInterceptor())));
      barrier.await(); // wait for all threads to be ready
      barrier.await(); // wait for all threads to finish
      log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
      for (Future<Void> future : futures) future.get();

      assert ic.containsInterceptorType(CallInterceptor.class);
      assert ic.containsInterceptorType(ActivationInterceptor.class);
      assert ic.containsInterceptorType(CacheMgmtInterceptor.class);
      assert ic.containsInterceptorType(DistCacheWriterInterceptor.class);
      assert ic.containsInterceptorType(InvalidationInterceptor.class);
      assert ic.asList().size() == 5 : "Resulting interceptor chain was actually " + ic.asList();
   }

   private static class InterceptorChainUpdater implements Callable<Void> {
      private final InterceptorChain ic;
      private final CyclicBarrier barrier;
      private final CommandInterceptor commandToExcercise;

      InterceptorChainUpdater(InterceptorChain ic, CyclicBarrier barrier, CommandInterceptor commandToExcercise) {
         this.ic = ic;
         this.barrier = barrier;
         this.commandToExcercise = commandToExcercise;
      }

      @Override
      public Void call() throws Exception {
         final Class<? extends CommandInterceptor> commandClass = commandToExcercise.getClass();
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();
            // test in a loop as the barrier is otherwise not enough to make sure
            // the different testing threads actually do make changes concurrently
            // 2000 is still almost nothing in terms of testsuite time.
            for (int i = 0; i < 2000; i++) {
               ic.removeInterceptor(commandClass);
               ic.addInterceptor(commandToExcercise, 1);
            }
            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }

}
