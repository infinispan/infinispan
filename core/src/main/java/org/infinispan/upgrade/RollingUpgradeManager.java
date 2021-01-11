package org.infinispan.upgrade;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
 * RollingUpgradeManager handles the synchronization of data between Infinispan
 * clusters when performing rolling upgrades.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 5.2
 */
@MBean(objectName = "RollingUpgradeManager", description = "Handles the migration of data when upgrading between versions.")
@Scope(value = Scopes.NAMED_CACHE)
@SurvivesRestarts
public class RollingUpgradeManager {
   private static final Log log = LogFactory.getLog(RollingUpgradeManager.class);
   private final ConcurrentMap<String, TargetMigrator> targetMigrators = new ConcurrentHashMap<>(2);
   @Inject Cache<Object, Object> cache;
   @Inject TimeService timeService;
   @Inject GlobalConfiguration globalConfiguration;

   @Start
   public void start() {
      ClassLoader cl = globalConfiguration.classLoader();
      for (TargetMigrator m : ServiceFinder.load(TargetMigrator.class, cl)) {
         targetMigrators.put(m.getName(), m);
      }
   }

   @ManagedOperation(
         description = "Synchronizes data from source clusters to target clusters with the specified migrator.",
         displayName = "Synchronizes data from source clusters to target clusters with the specified migrator."
   )
   public long synchronizeData(@Parameter(name="migratorName", description="Specifies the name of the migrator to use. Set hotrod as the value unless using custom migrators.") String migratorName) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      long start = timeService.time();
      long count = migrator.synchronizeData(cache);
      log.entriesMigrated(count, cache.getName(), Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
      return count;

   }

   @ManagedOperation(
           description = "Synchronizes data from source clusters to target clusters with the specified migrator.",
           displayName = "Synchronizes data from source clusters to target clusters with the specified migrator."
   )
   public long synchronizeData(@Parameter(name = "migratorName", description = "Specifies the name of the migrator to use. Set hotrod as the value unless using custom migrators.") String migratorName,
                               @Parameter(name = "readBatch", description = "Specifies how many entries to read at a time from source clusters. Default is 10000.") int readBatch,
                               @Parameter(name = "threads", description = "Specifies the number of threads to use per node when writing data to target clusters. Defaults to number of available processors.") int threads) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      long start = timeService.time();
      long count = migrator.synchronizeData(cache, readBatch, threads);
      log.entriesMigrated(count, cache.getName(), Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
      return count;
   }

   @ManagedOperation(
         description = "Disconnects target clusters from source clusters.",
         displayName = "Disconnects target clusters from source clusters."
   )
   public void disconnectSource(@Parameter(name="migratorName", description="Specifies the name of the migrator to use. Set hotrod as the value unless using custom migrators.") String migratorName) throws Exception {
      TargetMigrator migrator = getMigrator(migratorName);
      migrator.disconnectSource(cache);
   }

   private TargetMigrator getMigrator(String name) throws Exception {
      TargetMigrator targetMigrator = targetMigrators.get(name);
      if (targetMigrator == null) {
         throw CONTAINER.unknownMigrator(name);
      }
      return targetMigrator;
   }
}
