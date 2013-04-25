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
package org.infinispan.server.memcached

import org.infinispan.server.core.CacheValue
import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}
import org.infinispan.marshall.AbstractExternalizer
import scala.collection.JavaConversions._
import java.lang.StringBuilder

/**
 * Memcached value part of key/value pair containing flags on top the common
 * byte array and version.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the AdvancedExternalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@serializable
// TODO: Create a Metadata extension for Memcached value and add flags, removing this class
class MemcachedValue(override val data: Array[Byte], override val version: Long, val flags: Long)
      extends CacheValue(data, version) {

   override def toString = {
      new StringBuilder().append("MemcachedValue").append("{")
         .append("data=").append(Util.toStr(data))
         .append(", version=").append(version)
         .append(", flags=").append(flags)
         .append("}").toString
   }

}

object MemcachedValue {
   class Externalizer extends AbstractExternalizer[MemcachedValue] {
      override def writeObject(output: ObjectOutput, cacheValue: MemcachedValue) {
         output.writeInt(cacheValue.data.length)
         output.write(cacheValue.data)
         output.writeLong(cacheValue.version)
         output.writeLong(cacheValue.flags)
      }

      override def readObject(input: ObjectInput): MemcachedValue = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data)
         val version = input.readLong
         val flags = input.readLong
         new MemcachedValue(data, version, flags)
      }

      override def getTypeClasses =
         asJavaSet(Set[java.lang.Class[_ <: MemcachedValue]](classOf[MemcachedValue]))
   }
}