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

import org.infinispan.Metadata
import org.infinispan.container.versioning.EntryVersion
import org.infinispan.Metadata.Builder
import org.infinispan.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
case class MemcachedMetadata(
   flags: Long,
   lifespan: Long, maxIdle: Long,
   version: EntryVersion = null, builder: Builder = null) extends Metadata

object MemcachedMetadata {

   class Externalizer extends AbstractExternalizer[MemcachedMetadata] {

      def readObject(input: ObjectInput): MemcachedMetadata = {
         val flags = input.readLong()
         val lifespan = input.readLong()
         val maxIdle = input.readLong()
         val entryVersion = input.readObject().asInstanceOf[EntryVersion]
         MemcachedMetadata(flags, lifespan, maxIdle, entryVersion)
      }

      def writeObject(output: ObjectOutput, meta: MemcachedMetadata) {
         output.writeLong(meta.flags)
         output.writeLong(meta.lifespan)
         output.writeLong(meta.maxIdle)
         output.writeObject(meta.version)
      }

      def getTypeClasses: util.Set[Class[_ <: MemcachedMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MemcachedMetadata]](classOf[MemcachedMetadata]))

   }

}