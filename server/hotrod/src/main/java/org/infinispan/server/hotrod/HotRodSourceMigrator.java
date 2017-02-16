package org.infinispan.server.hotrod;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.EmbeddedMetadata.Builder;
import org.infinispan.metadata.Metadata;
import org.infinispan.upgrade.SourceMigrator;

/**
 * An implementation of Migrator, that understands the Hot Rod key and value formats.
 *
 * @author Manik Surtani
 * @since 5.2
 */
class HotRodSourceMigrator implements SourceMigrator {
   private final static String KNOWN_KEY = "___MigrationManager_HotRod_KnownKeys___";

   private final AdvancedCache<byte[], byte[]> cache;
   private final Marshaller marshaller;

   @Override
   public String getCacheName() {
      return cache.getName();
   }

   HotRodSourceMigrator(AdvancedCache<byte[], byte[]> cache) {
      this.cache = cache;
      this.marshaller = new GenericJBossMarshaller(); // TODO: Hard coded!  Yuck!  Assumes the Synchronizer service will use the same marshaller.  Doesn't matter what actual clients use to store/retrieve data.
   }

   @Override
   public void recordKnownGlobalKeyset() {
      try {
         // TODO: the bulk of this code is reusable across different implementations of Migrator
         byte[] bak = marshaller.objectToByteBuffer(KNOWN_KEY);

         Set<byte[]> allKeys = cache.keySet();
         Set<byte[]> copy = allKeys.stream().collect(Collectors.toSet());

         // Remove KNOWN_KEY from the key set - just in case it is there from a previous run.
         copy.remove(bak);

         // we cannot store the Set as it is; it will break if attempting to be read via Hot Rod.  This should be wrapped
         // in a CacheValue.
         Metadata metadata = new Builder().version(new NumericVersion(1)).build();
         cache.put(bak, marshaller.objectToByteBuffer(copy), metadata);
      } catch (InterruptedException | IOException e) {
         throw new CacheException(e);
      }
   }
}
