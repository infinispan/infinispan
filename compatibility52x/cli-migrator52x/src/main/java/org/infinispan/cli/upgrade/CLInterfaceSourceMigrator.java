package org.infinispan.cli.upgrade;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.upgrade.SourceMigrator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class CLInterfaceSourceMigrator implements SourceMigrator {

   private static final String KNOWN_KEY = "___MigrationManager_CLI_KnownKeys___";

   private final Cache<String, Set<Object>> cache;

   public CLInterfaceSourceMigrator(Cache<?, ?> cache) {
      this.cache = (Cache<String, Set<Object>>) cache;
   }

   @Override
   public void recordKnownGlobalKeyset() {
      try {
         CacheMode cm = cache.getCacheConfiguration().clustering().cacheMode();
         Set<Object> keys;
         if (cm.isReplicated() || !cm.isClustered()) {
            // If cache mode is LOCAL or REPL, dump local keyset.
            // Defensive copy to serialize and transmit across a network
            keys = new HashSet<Object>(cache.keySet());
         } else {
            // If cache mode is DIST, use a map/reduce task
            DistributedExecutorService des = new DefaultExecutorService(cache);
            List<Future<Set<Object>>> keysets = des.submitEverywhere(new GlobalKeysetTask(cache));
            Set<Object> combinedKeyset = new HashSet<Object>();

            for (Future<Set<Object>> keyset : keysets)
               combinedKeyset.addAll(keyset.get());

            keys = combinedKeyset;
         }

         // Remove KNOWN_KEY from the key set - just in case it is there from a previous run.
         keys.remove(KNOWN_KEY);

         cache.put(KNOWN_KEY, keys);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt(); // reset
      } catch (ExecutionException e) {
         throw new CacheException("Unable to record all known keys", e);
      }
   }

   @Override
   public String getCacheName() {
      return cache.getName();
   }

   static class GlobalKeysetTask implements DistributedCallable<Object, Object, Set<Object>> {

      final Cache<?, ?> cache;

      GlobalKeysetTask(Cache<?, ?> cache) {
         this.cache = cache;
      }

      @Override
      public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
         // TODO: Customise this generated block
      }

      @Override
      public Set<Object> call() throws Exception {
         // Defensive copy to serialize and transmit across a network
         return new HashSet<Object>(cache.keySet());
      }
   }

}
