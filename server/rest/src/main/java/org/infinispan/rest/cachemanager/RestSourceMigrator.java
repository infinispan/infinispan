/**
 * An implementation of SourceMigrator for the REST server
 *
 * @author Tristan Tarrant
 * @since 6.0
 */

package org.infinispan.rest.cachemanager;

import org.infinispan.Cache;
import org.infinispan.upgrade.SourceMigrator;

class RestSourceMigrator<V> implements SourceMigrator {
   private final Cache<String, V> cache;

   RestSourceMigrator(Cache<String, V> cache) {
      this.cache = cache;
   }

   @Override
   public String getCacheName() {
      return cache.getName();
   }

}
