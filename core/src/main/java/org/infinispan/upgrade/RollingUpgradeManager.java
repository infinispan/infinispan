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

package org.infinispan.upgrade;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.util.TimeService;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This component handles the control hooks to handle migrating from one version of Infinispan to
 * another.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.2
 */
@MBean(objectName = "RollingUpgradeManager", description = "This component handles the control hooks to handle migrating data from one version of Infinispan to another")
@Scope(value = Scopes.NAMED_CACHE)
@SurvivesRestarts
public class RollingUpgradeManager {
   private static final Log log = LogFactory.getLog(RollingUpgradeManager.class);
   private final Set<SourceMigrator> sourceMigrators = new HashSet<SourceMigrator>(2);
   private Cache<Object, Object> cache;
   private TimeService timeService;

   @Inject
   public void initialize(final Cache<Object, Object> cache, TimeService timeService) {
      this.cache = cache;
      this.timeService = timeService;
   }

   @ManagedOperation(
         description = "Dumps the global known keyset to a well-known key for retrieval by the upgrade process",
         displayName = "Dumps the global known keyset"
   )
   public void recordKnownGlobalKeyset() {
      for (SourceMigrator m : sourceMigrators)
         m.recordKnownGlobalKeyset();
   }

   @ManagedOperation(
         description = "Synchronizes data from the old cluster to this using the specified migrator",
         displayName = "Synchronizes data from the old cluster to this using the specified migrator"
   )
   public long synchronizeData(@Parameter(name="migratorName", description="The name of the migrator to use") String migratorName) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      long start = timeService.time();
      long count = migrator.synchronizeData(cache);
      log.entriesMigrated(count, cache.getName(), Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
      return count;

   }

   @ManagedOperation(
         description = "Disconnects the target cluster from the source cluster according to the specified migrator",
         displayName = "Disconnects the target cluster from the source cluster"
   )
   public void disconnectSource(@Parameter(name="migratorName", description="The name of the migrator to use") String migratorName) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      migrator.disconnectSource(cache);
   }

   private TargetMigrator getMigrator(String name) throws Exception {
      ClassLoader cl = cache.getCacheManager().getCacheManagerConfiguration().classLoader();
      for (TargetMigrator m : ServiceLoader.load(TargetMigrator.class, cl)) {
         if (name.equalsIgnoreCase(m.getName())) {
            return m;
         }
      }
      throw log.unknownMigrator(name);
   }

   /**
    * Registers a migrator for a specific data format or endpoint. In the Infinispan ecosystem, we'd
    * typically have one Migrator implementation for Hot Rod, one for memcached, one for REST and
    * one for embedded/in-VM mode, and these would typically be added to the upgrade manager on
    * first access via any of these protocols.
    *
    * @param migrator
    */
   public void addSourceMigrator(SourceMigrator migrator) {
      sourceMigrators.add(migrator);
   }
}
