/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.server.hotrod.test

import org.infinispan.Cache
import java.util.Random
import org.infinispan.marshall.jboss.JBossMarshaller

/**
 * {@link org.infinispan.distribution.MagicKey} equivalent for HotRod
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
object HotRodMagicKeyGenerator {

   def newKey(cache: Cache[AnyRef, AnyRef]): Array[Byte] = {
      val ch = cache.getAdvancedCache.getDistributionManager.getReadConsistentHash
      val nodeAddress = cache.getAdvancedCache.getRpcManager.getAddress
      val r = new Random()

      val sm = new JBossMarshaller()
      for (i <- 0 to 1000) {
         val candidate = String.valueOf(r.nextLong())
         val candidateBytes = sm.objectToByteBuffer(candidate, 64)
         if (ch.isKeyLocalToNode(nodeAddress, candidateBytes)) {
            return candidateBytes
         }
      }

      throw new RuntimeException("Unable to find a key local to node " + cache)
   }

   def getStringObject(bytes: Array[Byte]): String = {
      val sm = new JBossMarshaller()
      sm.objectFromByteBuffer(bytes).asInstanceOf[String]
   }

}
