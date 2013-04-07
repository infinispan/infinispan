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

import org.infinispan.util.Util
import java.io.{ObjectOutput, ObjectInput}
import java.util.Arrays
import org.infinispan.marshall.AbstractExternalizer
import scala.collection.JavaConversions._
import java.lang.StringBuilder

/**
 * Represents the value part of a key/value pair stored in a protocol cache.
 * With each value, a version is stored which allows for conditional operations
 * to be executed remotely in a efficient way.  For more detailed info on
 * conditional operations, check <a href="http://community.jboss.org/docs/DOC-15604">this document</a>.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the externalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 * @deprecated
 */
@serializable
@deprecated
class CacheValue(val data: Array[Byte], val version: Long) {

   override def toString = {
      new StringBuilder().append("CacheValue").append("{")
         .append("data=").append(Util.printArray(data, false))
         .append(", version=").append(version)
         .append("}").toString
   }

   override def equals(obj: Any) = {
      obj match {
         // Apparenlty this is the way arrays should be compared for equality of contents, see:
         // http://old.nabble.com/-scala--Array-equality-td23149094.html
         case k: CacheValue => Arrays.equals(k.data, this.data) && k.version == this.version
         case _ => false
      }
   }

   override def hashCode: Int = {
      41 + Arrays.hashCode(data)
   }
   
}

object CacheValue {
   class Externalizer extends AbstractExternalizer[CacheValue] {
      override def writeObject(output: ObjectOutput, cacheValue: CacheValue) {
         output.writeInt(cacheValue.data.length)
         output.write(cacheValue.data)
         output.writeLong(cacheValue.version)
      }

      override def readObject(input: ObjectInput): CacheValue = {
         val data = new Array[Byte](input.readInt())
         input.readFully(data) // Must be readFully, otherwise partial arrays can be read under load!
         val version = input.readLong
         new CacheValue(data, version)
      }

      override def getTypeClasses =
         asJavaSet(Set[java.lang.Class[_ <: CacheValue]](classOf[CacheValue]))
   }
}
