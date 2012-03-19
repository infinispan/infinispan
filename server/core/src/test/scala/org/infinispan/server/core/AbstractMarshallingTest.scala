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
package org.infinispan.server.core

import org.testng.annotations.{AfterClass, BeforeTest}
import java.util.Random
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import org.infinispan.config.GlobalConfiguration
import org.infinispan.test.TestingUtil
import org.infinispan.marshall.AbstractDelegatingMarshaller
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory

/**
 * Abstract class to help marshalling tests in different server modules.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractMarshallingTest {

   var marshaller : AbstractDelegatingMarshaller = _
   var cm : EmbeddedCacheManager = _

   @BeforeTest(alwaysRun=true)
   def setUp {
      // Manual addition of externalizers to replication what happens in fully functional tests
      val globalCfg = new GlobalConfiguration
      new LifecycleCallbacks().addExternalizer(globalCfg)
      cm = TestCacheManagerFactory.createLocalCacheManager(false)
      marshaller = TestingUtil.extractCacheMarshaller(cm.getCache())
   }

   @AfterClass(alwaysRun=true)
   def tearDown = cm.stop()

   protected def getBigByteArray: Array[Byte] = {
      val value = new String(randomByteArray(1000))
      val result = new ByteArrayOutputStream(1000)
      val oos = new ObjectOutputStream(result)
      oos.writeObject(value)
      result.toByteArray
   }

   private def randomByteArray(i: Int): Array[Byte] = {
      val r = new Random
      val result = new Array[Byte](i)
      r.nextBytes(result)
      result
   }

}