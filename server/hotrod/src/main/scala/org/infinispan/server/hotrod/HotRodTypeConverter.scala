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

package org.infinispan.server.hotrod

import org.infinispan.compat.TypeConverter
import org.infinispan.marshall.jboss.GenericJBossMarshaller
import org.infinispan.context.Flag
import org.infinispan.marshall.Marshaller

/**
 * Hot Rod type converter for compatibility mode.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class HotRodTypeConverter extends TypeConverter[Array[Byte], Array[Byte], AnyRef, AnyRef] {

   // Default marshaller is the one used by the Hot Rod client,
   // but can be configured for compatibility use cases
   private var marshaller: Marshaller = new GenericJBossMarshaller

   override def boxKey(key: Array[Byte]): AnyRef = unmarshall(key)

   override def boxValue(value: Array[Byte]): AnyRef = unmarshall(value)

   override def unboxValue(target: AnyRef): Array[Byte] = marshall(target)

   override def supportsInvocation(flag: Flag): Boolean =
      if (flag == Flag.OPERATION_HOTROD) true else false

   override def setMarshaller(marshaller: Marshaller) {
      this.marshaller = marshaller
   }

   private def unmarshall(source: Array[Byte]): AnyRef =
      if (source != null) marshaller.objectFromByteBuffer(source) else source

   private def marshall(source: AnyRef): Array[Byte] =
      if (source != null) marshaller.objectToByteBuffer(source) else null

}
