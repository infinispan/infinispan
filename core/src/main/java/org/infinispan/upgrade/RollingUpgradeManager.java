package org.infinispan.upgrade;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private final Set<SourceMigrator> sourceMigrators = new HashSet<>(2);
   @Inject private Cache<Object, Object> cache;
   @Inject private TimeService timeService;

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
           description = "Synchronizes data from the old cluster to this using the specified migrator",
           displayName = "Synchronizes data from the old cluster to this using the specified migrator"
   )
   public long synchronizeData(@Parameter(name = "migratorName", description = "The name of the migrator to use") String migratorName,
                               @Parameter(name = "readBatch", description = "Numbers of entries transferred at a time from the old cluster") int readBatch,
                               @Parameter(name = "threads", description = "Number of threads per node used to write data to the new cluster") int threads) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      long start = timeService.time();
      long count = migrator.synchronizeData(cache, readBatch, threads);
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
      for (TargetMigrator m : ServiceFinder.load(TargetMigrator.class, cl)) {
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
