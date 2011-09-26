/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod.stress

import java.lang.reflect.Method
import org.infinispan.test.TestingUtil.generateRandomString
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.HotRodSingleNodeTest
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test

/**
 * A simple test that stresses Hot Rod by storing big data and waits to allow
 * the test runner to generate heap dumps for the test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("stress"), testName = "server.hotrod.stress.HotRodBigDataStressTest", enabled = false)
class HotRodBigDataStressTest extends HotRodSingleNodeTest {

   def testPutBigSizeValue(m: Method) {
      val value = generateRandomString(1024 * 1024).getBytes
      assertStatus(client.put(k(m), 0, 0, value), Success)
      while (true)
         Thread.sleep(5000)
   }

}