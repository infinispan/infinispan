/**
 * An implementation of Migrator, that understands the Hot Rod key and value formats.
 *
 * @author Manik Surtani
 * @since 5.2
 */

package org.infinispan.server.hotrod

import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.upgrade.SourceMigrator
import org.infinispan.tasks.GlobalKeySetTask
import org.infinispan.metadata.EmbeddedMetadata
import EmbeddedMetadata.Builder
import org.infinispan.container.versioning.NumericVersion

class HotRodSourceMigrator(cache: Cache) extends SourceMigrator {
   val KNOWN_KEY = "___MigrationManager_HotRod_KnownKeys___"
   val MARSHALLER = new GenericJBossMarshaller // TODO: Hard coded!  Yuck!  Assumes the Synchronizer service will use the same marshaller.  Doesn't matter what actual clients use to store/retrieve data.

   @Override
   def getCacheName = cache.getName

   @Override
   def recordKnownGlobalKeyset() {
      // TODO: the bulk of this code is reusable across different implementations of Migrator
      val bak = MARSHALLER.objectToByteBuffer(KNOWN_KEY)

      val cm = SecurityActions.getCacheConfiguration(cache.getAdvancedCache).clustering.cacheMode
      val keys = GlobalKeySetTask.getGlobalKeySet(cache)

      // Remove KNOWN_KEY from the key set - just in case it is there from a previous run.
      keys remove KNOWN_KEY

      // we cannot store the Set as it is; it will break if attempting to be read via Hot Rod.  This should be wrapped
      // in a CacheValue.
      val metadata = new Builder().version(new NumericVersion(1)).build()
      cache.put(bak, MARSHALLER.objectToByteBuffer(keys), metadata)
   }
}
