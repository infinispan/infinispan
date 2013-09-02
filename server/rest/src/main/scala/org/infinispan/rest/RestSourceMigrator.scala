/**
 * An implementation of SourceMigrator for the REST server
 *
 * @author Tristan Tarrant
 * @since 6.0
 */

package org.infinispan.server.hotrod

import org.infinispan.Cache
import org.infinispan.distexec.{DistributedCallable, DefaultExecutorService}
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.upgrade.SourceMigrator
import org.infinispan.tasks.GlobalKeySetTask
import org.infinispan.metadata.EmbeddedMetadata
import EmbeddedMetadata.Builder
import org.infinispan.container.versioning.NumericVersion

class RestSourceMigrator(cache: Cache[String, Array[Byte]]) extends SourceMigrator {

   @Override
   def getCacheName = cache.getName

   @Override
   def recordKnownGlobalKeyset() {
      // Nothing to do, the REST server already properly allows retrieving the full keyset
   }
}
