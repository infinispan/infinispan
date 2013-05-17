/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.compat.TypeConverter
import org.infinispan.context.Flag
import org.infinispan.marshall.Marshaller

/**
 * Type converter that transforms Memcached data so that it can be accessible
 * via other endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MemcachedTypeConverter extends TypeConverter[String, Array[Byte], String, AnyRef] {

   private var marshaller: Marshaller = _

   override def boxKey(key: String): String = key

   override def boxValue(value: Array[Byte]): AnyRef = unmarshall(value)

   override def unboxValue(target: AnyRef): Array[Byte] = marshall(target)

   override def setMarshaller(marshaller: Marshaller) {
      this.marshaller = marshaller
   }

   override def supportsInvocation(flag: Flag): Boolean =
      if (flag == Flag.OPERATION_MEMCACHED) true else false

   private def unmarshall(source: Array[Byte]): AnyRef =
      if (source != null) marshaller.objectFromByteBuffer(source) else source

   private def marshall(source: AnyRef): Array[Byte] =
      if (source != null) marshaller.objectToByteBuffer(source) else null

}
