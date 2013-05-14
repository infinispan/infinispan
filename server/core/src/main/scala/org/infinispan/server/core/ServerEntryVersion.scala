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

package org.infinispan.server.core

import org.infinispan.container.versioning.{InequalVersionComparisonResult, EntryVersion}
import org.infinispan.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet

/**
 * Server-endpoint version
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
case class ServerEntryVersion(version: Long) extends EntryVersion {

   def compareTo(other: EntryVersion): InequalVersionComparisonResult = {
      other match {
         case ServerEntryVersion(otherVersion) =>
            otherVersion match {
               case v if version < v => InequalVersionComparisonResult.BEFORE
               case v if version > v => InequalVersionComparisonResult.AFTER
               case _ => InequalVersionComparisonResult.EQUAL
            }
         case _ =>
            val className = other.getClass.getName
            throw new IllegalArgumentException(
                  s"Unable to compare other types: $className")
      }
   }

}

object ServerEntryVersion {

   class Externalizer extends AbstractExternalizer[ServerEntryVersion] {

      override def readObject(input: ObjectInput): ServerEntryVersion =
         ServerEntryVersion(input.readLong())

      override def writeObject(output: ObjectOutput, entryVersion: ServerEntryVersion) {
         output.writeLong(entryVersion.version)
      }

      def getTypeClasses: util.Set[Class[_ <: ServerEntryVersion]] =
         setAsJavaSet(Set[java.lang.Class[_ <: ServerEntryVersion]](classOf[ServerEntryVersion]))
   }

}
