/**
 * An implementation of SourceMigrator for the REST server
 *
 * @author Tristan Tarrant
 * @since 6.0
 */

package org.infinispan.rest;

import org.infinispan.Cache;
import org.infinispan.upgrade.SourceMigrator;

class RestSourceMigrator implements SourceMigrator {
   private final Cache<String, byte[]> cache;

   RestSourceMigrator(Cache<String, byte[]> cache) {
      this.cache = cache;
   }

   @Override
   public String getCacheName() {
      return cache.getName();
   }

   @Override
   public void recordKnownGlobalKeyset() {
      // Nothing to do, the REST server already properly allows retrieving the full keyset
   }
}
