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
import org.infinispan.commands.remote.ClusteredGetCommand
import org.infinispan.server.core.AbstractMarshallingTest
import org.infinispan.api.BasicCacheContainer
import org.infinispan.util.{ByteArrayEquivalence, InfinispanCollections}

/**
 * Tests marshalling of Hot Rod classes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodMarshallingTest")
class HotRodMarshallingTest extends AbstractMarshallingTest {

   def testMarshallingBigByteArrayKey() {
      val cacheKey = getBigByteArray
      val bytes = marshaller.objectToByteBuffer(cacheKey)
      val readKey = marshaller.objectFromByteBuffer(bytes).asInstanceOf[Array[Byte]]
      assertEquals(readKey, cacheKey)
   }

   def testMarshallingCommandWithBigByteArrayKey() {
      val cacheKey = getBigByteArray
      val command = new ClusteredGetCommand(cacheKey,
         BasicCacheContainer.DEFAULT_CACHE_NAME, InfinispanCollections.emptySet(), false, null,
         ByteArrayEquivalence.INSTANCE)
      val bytes = marshaller.objectToByteBuffer(command)
      val readCommand = marshaller.objectFromByteBuffer(bytes).asInstanceOf[ClusteredGetCommand]
      assertEquals(readCommand, command)
   }

}
