package org.infinispan.server.hotrod;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.testng.annotations.Test;

/**
 * Tests that Hot Rod servers can be concurrently accessed and modified.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConcurrentTest")
public class HotRodConcurrentTest extends HotRodSingleNodeTest {

   public void testConcurrentPutRequests(Method m) throws Exception {
      int numClients = 10;
      int numOpsPerClient = 100;
      CyclicBarrier barrier = new CyclicBarrier(numClients + 1);
      List<Future<Void>> futures = new ArrayList<>();
      List<Operator> operators = new ArrayList<>();
      try {
         for (int i = 0; i < numClients; i++) {
            Operator operator = new Operator(barrier, m, i, numOpsPerClient);
            operators.add(operator);
            Future<Void> future = fork(operator);
            futures.add(future);
         }
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish

         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<?> future : futures) {
            future.get();
         }
      } finally {
         for (Operator operator : operators) {
            operator.stop();
         }
      }
   }

   class Operator implements Callable<Void> {

      private final CyclicBarrier barrier;
      private final Method m;
      private final int clientId;
      private final int numOpsPerClient;

      public Operator(CyclicBarrier barrier, Method m, int clientId, int numOpsPerClient) {
         this.barrier = barrier;
         this.m = m;
         this.clientId = clientId;
         this.numOpsPerClient = numOpsPerClient;
      }

      private HotRodClient client = new HotRodClient("127.0.0.1", server().getPort(), cacheName, (byte) 20);

      @Override
      public Void call() throws Exception {
         TestResourceTracker.testThreadStarted(HotRodConcurrentTest.this.getTestName());
         log.debug("Wait for all executions paths to be ready to perform calls");
         barrier.await();
         try {
            for (int i = 0; i < numOpsPerClient; i++) {
               client().assertPut(m, "k" + clientId + "-" + i + "-", "v" + clientId + "-" + i + "-");
            }
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
         return null;
      }

      public Future<?> stop() {
         return client.stop();
      }
   }
}
