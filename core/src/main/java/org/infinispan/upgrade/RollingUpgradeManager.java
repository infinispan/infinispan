package org.infinispan.upgrade;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
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
   private final ConcurrentMap<String, TargetMigrator> targetMigrators = new ConcurrentHashMap<>(2);
   @Inject private Cache<Object, Object> cache;
   @Inject private TimeService timeService;
   @Inject private GlobalConfiguration globalConfiguration;

   @Start
   public void start() {
      ClassLoader cl = globalConfiguration.classLoader();
      for (TargetMigrator m : ServiceFinder.load(TargetMigrator.class, cl)) {
         targetMigrators.put(m.getName(), m);
      }
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
      TargetMigrator targetMigrator = targetMigrators.get(name);
      if (targetMigrator == null) {
         throw log.unknownMigrator(name);
      }
      return targetMigrator;
   }
}
