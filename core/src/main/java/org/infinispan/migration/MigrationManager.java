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

package org.infinispan.migration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * This component handles the control hooks to handle migrating from one version of Infinispan to another.
 *
 * @author Manik Surtani
 * @since 5.2
 */
@MBean(objectName = "MigrationManager", description = "This component handles the control hooks to handle migrating from one version of Infinispan to another")
public class MigrationManager {
   private static final String KNOWN_KEY = "___MigrationManager___KnownKeys___";
   private Cache cache;
   private static final Log log = LogFactory.getLog(MigrationManager.class);

   @Inject
   private void inject(Cache cache) {
      this.cache = cache;
   }

   @ManagedOperation(description = "Dumps the global known keyset to a well-known key for retrieval by the migration process")
   @Operation(displayName = "Dumps the global known keyset to a well-known key for retrieval by the migration process")
   public void recordKnownGlobalKeyset() {
      recordKnownGlobalKeyset(KNOWN_KEY);
   }

   @ManagedOperation(description = "Dumps the global known keyset to a specified key for retrieval by the migration process")
   @Operation(displayName = "Dumps the global known keyset to a specified key for retrieval by the migration process")
   public void recordKnownGlobalKeyset(String keyToRecordKnownKeySet) {

      // TODO: this currently is a Hot Rod readable key only.  Create more options to store this in a plain string
      // format for reading via REST or memcached too.

      ByteArrayKey bak = new ByteArrayKey(keyToRecordKnownKeySet.getBytes(Charset.forName("UTF-8")));

      CacheMode cm = cache.getCacheConfiguration().clustering().cacheMode();
      if (cm.isReplicated() || !cm.isClustered()) {
         // If cache mode is LOCAL or REPL, dump local keyset.
         cache.put(bak, cache.keySet());
      } else {
         // If cache mode is DIST, use a map/reduce task
         DistributedExecutorService des = new DefaultExecutorService(cache);
         List<Future<Set<ByteArrayKey>>> keysets = des.submitEverywhere(new GlobalKeysetTask());
         Set<ByteArrayKey> combinedKeyset = new HashSet<ByteArrayKey>();
         try {
            for (Future<Set<ByteArrayKey>> f: keysets) combinedKeyset.addAll(f.get());
         } catch (Exception e) {
            log.warn("Unable to complete task!", e);
         }
         cache.put(bak, combinedKeyset);
      }
   }

   private static class GlobalKeysetTask implements DistributedCallable<ByteArrayKey, Object, Set<ByteArrayKey>> {
      private Cache<ByteArrayKey, Object> cache;

      @Override
      public Set<ByteArrayKey> call() throws Exception {
         return cache.keySet();
      }

      @Override
      public void setEnvironment(Cache<ByteArrayKey, Object> cache, Set<ByteArrayKey> inputKeys) {
         this.cache = cache;
      }
   }
}
