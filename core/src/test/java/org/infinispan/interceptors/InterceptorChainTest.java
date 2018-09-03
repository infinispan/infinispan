package org.infinispan.interceptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.AsyncInterceptorChainImpl;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
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
      componentMetadataRepo.initialize(Collections.emptyList(), InterceptorChainTest.class.getClassLoader());
      AsyncInterceptorChainImpl asyncInterceptorChain =
            new AsyncInterceptorChainImpl(componentMetadataRepo);
      GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().build();
      InterceptorChain ic = new InterceptorChain();
      TestingUtil.inject(ic, asyncInterceptorChain);
      ic.setFirstInChain(new DummyCallInterceptor());
      ic.addInterceptor(new DummyActivationInterceptor(), 1);
      CyclicBarrier barrier = new CyclicBarrier(4);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(2);
      // We do test concurrent add/remove of different types per thread,
      // so that the final result is predictable (testable) and that we
      // can not possibly fail because of the InterceptorChain checking
      // that no interceptor is ever added twice.
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new DummyCacheMgmtInterceptor())));
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new DummyDistCacheWriterInterceptor())));
      futures.add(fork(new InterceptorChainUpdater(ic, barrier, new DummyInvalidationInterceptor())));
      barrier.await(); // wait for all threads to be ready
      barrier.await(); // wait for all threads to finish
      log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
      for (Future<Void> future : futures) future.get();

      assert ic.containsInterceptorType(DummyCallInterceptor.class);
      assert ic.containsInterceptorType(DummyActivationInterceptor.class);
      assert ic.containsInterceptorType(DummyCacheMgmtInterceptor.class);
      assert ic.containsInterceptorType(DummyDistCacheWriterInterceptor.class);
      assert ic.containsInterceptorType(DummyInvalidationInterceptor.class);
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

   private static class DummyCallInterceptor extends CommandInterceptor {
   }

   private static class DummyActivationInterceptor extends CommandInterceptor {
   }

   private static class DummyCacheMgmtInterceptor extends CommandInterceptor {
   }

   private static class DummyDistCacheWriterInterceptor extends CommandInterceptor {
   }

   private static class DummyInvalidationInterceptor extends CommandInterceptor {
   }
}
