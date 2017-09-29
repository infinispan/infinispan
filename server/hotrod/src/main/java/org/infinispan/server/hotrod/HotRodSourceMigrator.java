package org.infinispan.server.hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.upgrade.SourceMigrator;

/**
 * An implementation of Migrator, that understands the Hot Rod key and value formats.
 *
 * @author Manik Surtani
 * @since 5.2
 */
class HotRodSourceMigrator implements SourceMigrator {

   private final AdvancedCache<byte[], byte[]> cache;

   @Override
   public String getCacheName() {
      return cache.getName();
   }

   HotRodSourceMigrator(AdvancedCache<byte[], byte[]> cache) {
      this.cache = cache;
   }

}
