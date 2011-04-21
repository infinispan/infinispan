/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.util.Util
import java.util.Arrays
import org.infinispan.marshall.Marshallable
import java.io.{ObjectInput, ObjectOutput}
import org.infinispan.server.core.Logging

/**
 * Represents the key part of a key/value pair stored in the underlying Hot Rod cache.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
// TODO: putting Ids.HOTROD_CACHE_KEY fails compilation in 2.8 - https://lampsvn.epfl.ch/trac/scala/ticket/2764
@Marshallable(externalizer = classOf[CacheKey.Externalizer], id = 57)
final class CacheKey(val data: Array[Byte]) {

   override def equals(obj: Any) = {
      obj match {
         // Apparenlty this is the way arrays should be compared for equality of contents, see:
         // http://old.nabble.com/-scala--Array-equality-td23149094.html
         case k: CacheKey => Arrays.equals(k.data, this.data)
         case _ => false
      }
   }

   override def hashCode: Int = {
      41 + Arrays.hashCode(data)
   }

   override def toString = {
      new StringBuilder().append("CacheKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString
   }

}

object CacheKey extends Logging {
   class Externalizer extends org.infinispan.marshall.Externalizer {
      override def writeObject(output: ObjectOutput, obj: AnyRef) {
         val cacheKey = obj.asInstanceOf[CacheKey]
         output.writeInt(cacheKey.data.length)
         output.write(cacheKey.data)
      }

      override def readObject(input: ObjectInput): AnyRef = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data)
         new CacheKey(data)
      }
   }
}