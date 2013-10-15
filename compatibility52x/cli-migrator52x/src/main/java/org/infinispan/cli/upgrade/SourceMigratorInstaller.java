package org.infinispan.cli.upgrade;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class SourceMigratorInstaller extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(SourceMigratorInstaller.class);

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Cache<?, ?> cache = cr.getComponent(Cache.class);
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) {
         log.debug("Register CLI source migrator");
         migrationManager.addSourceMigrator(new CLInterfaceSourceMigrator(cache));
      }
   }

}
