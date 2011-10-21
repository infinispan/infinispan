/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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

      private lazy val client = new HotRodClient("127.0.0.1", server.getPort, cacheName, 60, 10)

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