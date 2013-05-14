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

package org.infinispan.rest

import org.infinispan.Metadata
import org.infinispan.container.versioning.EntryVersion
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import java.util.concurrent.TimeUnit
import org.infinispan.Metadata.Builder
import org.infinispan.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet

/**
 * Metadata for MIME data stored in REST servers.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
case class MimeMetadata(
        contentType: String,
        lifespanParam: Long, lifespanUnit: TimeUnit,
        memcachedParam: Long, maxIdleUnit: TimeUnit,
        version: EntryVersion = null, builder: Builder = null) extends Metadata {

   override val lifespan = lifespanUnit.toMillis(lifespanParam)
   override val maxIdle = maxIdleUnit.toMillis(memcachedParam)

}

object MimeMetadata {

   class Externalizer extends AbstractExternalizer[MimeMetadata] {

      def readObject(input: ObjectInput): MimeMetadata = {
         val contentType = input.readUTF()
         val lifespan = input.readLong()
         val maxIdle = input.readLong()
         MimeMetadata(contentType, lifespan, MILLIS, maxIdle, MILLIS)
      }

      def writeObject(output: ObjectOutput, meta: MimeMetadata) {
         output.writeUTF(meta.contentType)
         output.writeLong(meta.lifespan)
         output.writeLong(meta.maxIdle)
      }

      def getTypeClasses: util.Set[Class[_ <: MimeMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MimeMetadata]](classOf[MimeMetadata]))

   }

}
