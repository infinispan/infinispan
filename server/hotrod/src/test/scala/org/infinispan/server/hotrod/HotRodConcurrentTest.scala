package org.infinispan.server.hotrod

import java.lang.reflect.Method
import java.util.concurrent.{Callable, Executors, Future, CyclicBarrier}
import test.HotRodClient
import org.testng.annotations.Test

/**
 * Tests that Hot Rod servers can be concurrently accessed and modified.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConcurrentTest")
class HotRodConcurrentTest extends HotRodSingleNodeTest {

   def testConcurrentPutRequests(m: Method) {
      val numClients = 10
      val numOpsPerClient = 100
      val barrier = new CyclicBarrier(numClients + 1)
      val executorService = Executors.newCachedThreadPool
      var futures: List[Future[Unit]] = List()
      var operators: List[Operator] = List()
      try {
         for (i <- 0 until numClients) {
            val operator = new Operator(barrier, m, i, numOpsPerClient)
            operators = operator :: operators
            val future = executorService.submit(operator)
            futures = future :: futures
         }
         barrier.await // wait for all threads to be ready
         barrier.await // wait for all threads to finish

         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported")
         for (future <- futures) future.get
      }
      finally {
         for (operator <- operators) operator.stop
      }
   }

   class Operator(barrier: CyclicBarrier, m: Method, clientId: Int, numOpsPerClient: Int) extends Callable[Unit] {

      private lazy val client = new HotRodClient("127.0.0.1", server.getPort, cacheName, 60, 20)

      override def call {
         log.debug("Wait for all executions paths to be ready to perform calls")
         barrier.await
         try {
            for (i <- 0 until numOpsPerClient) {
               client.assertPut(m, "k" + clientId + "-" + i + "-", "v" + clientId + "-" + i + "-")
            }
         } finally {
            log.debug("Wait for all execution paths to finish")
            barrier.await
         }
      }

      def stop = client.stop
   }
}