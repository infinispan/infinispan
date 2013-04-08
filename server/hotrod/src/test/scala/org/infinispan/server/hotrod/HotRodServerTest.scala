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

import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.server.core.test.Stoppable
import org.infinispan.test.fwk.TestCacheManagerFactory

/**
 * Hot Rod server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodServerTest")
class HotRodServerTest {

   def testValidateProtocolServerNullProperties {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager()) { cm =>
         Stoppable.useServer(new HotRodServer) { server =>
            server.startWithProperties(null, cm)
            assertEquals(server.getHost, "127.0.0.1")
            assertEquals(server.getPort, 11222)
         }
      }
   }

}
