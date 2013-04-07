/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

/**
 * An implementation of Migrator, that understands the Hot Rod key and value formats.
 *
 * @author Manik Surtani
 * @since 5.2
 */

package org.infinispan.server.hotrod

import org.infinispan.Cache
import org.infinispan.distexec.{DistributedCallable, DefaultExecutorService}
import org.infinispan.marshall.jboss.GenericJBossMarshaller
import org.infinispan.upgrade.SourceMigrator
import org.infinispan.tasks.GlobalKeySetTask
import org.infinispan.EmbeddedMetadata.Builder
import org.infinispan.container.versioning.NumericVersion

class HotRodSourceMigrator(cache: Cache[Array[Byte], Array[Byte]]) extends SourceMigrator {
   val KNOWN_KEY = "___MigrationManager_HotRod_KnownKeys___"
   val MARSHALLER = new GenericJBossMarshaller // TODO: Hard coded!  Yuck!  Assumes the Synchronizer service will use the same marshaller.  Doesn't matter what actual clients use to store/retrieve data.

   @Override
   def getCacheName = cache.getName

   @Override
   def recordKnownGlobalKeyset() {
      // TODO: Maybe we should allow passing in of a well-known key prefix, and return the generated key to use?
      recordKnownGlobalKeyset(KNOWN_KEY)
   }

   def recordKnownGlobalKeyset(keyToRecordKnownKeySet: String) {
      // TODO: the bulk of ths code is reusable across different implementations of Migrator
      val bak = MARSHALLER.objectToByteBuffer(keyToRecordKnownKeySet)

      val cm = cache.getCacheConfiguration().clustering().cacheMode()
      val keys = GlobalKeySetTask.getGlobalKeySet(cache)

      // Remove KNOWN_KEY from the key set - just in case it is there from a previous run.
      keys remove KNOWN_KEY

      // we cannot store the Set as it is; it will break if attempting to be read via Hot Rod.  This should be wrapped
      // in a CacheValue.
      val metadata = new Builder().version(new NumericVersion(1)).build()
      cache.getAdvancedCache.put(bak, MARSHALLER.objectToByteBuffer(keys), metadata)
   }
}
