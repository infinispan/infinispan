package org.infinispan.cdc;

import static org.infinispan.cdc.logging.Log.CONTAINER;
import static org.infinispan.commons.logging.Log.CONFIG;

import java.sql.SQLException;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfiguration;
import org.infinispan.cdc.internal.configuration.CompleteConfiguration;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Module for enabling change data capture directly into a cache.
 *
 * <p>
 * This module gives the capability of enabling change data capture directly into a cache. The module requires prior
 * configuration on the database side, but after everything is done, enabling the capability into a cache is as simple
 * as adding the database configuration:
 *
 * <pre>
 * {@code
 * <distributed-cache name="table_name">
 *    <change-data-capture>
 *       <simple-connection
 *          username="database-user"
 *          password="database-pass"
 *          connection-url="jdbc:..."
 *          driver="Driver.class" />
 *    </change-data-capture>
 * </distributed-cache>
 * }
 * </pre>
 *
 * This functionality captures changes applied in the `<code>table_name</code>` table in the database and updates the
 * cache with the recent events in the same order. This avoids the need to externally synchronize the cache and reduces
 * the time with stale data. The cache's contents won't diverge from the database values since the updates are applied
 * in the same order. The cache should provide a view from the database with very low delay between updates.
 * </p>
 *
 * <br></br>
 *
 * <h2>Pre-loading</h2>
 * <p>
 * The module will also read the data for the table and populate the cache automatically during start. Any subsequent
 * change applied to the database will reflect in the cache with very-low delays.
 * </p>
 *
 * @author José Bolina
 * @since 16.0
 */
@InfinispanModule(name = "change-data-capture", requiredModules = "core")
public final class ChangeDataCaptureModule implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(ChangeDataCaptureModule.class);

   public static final String CDC_FEATURE = "change-data-capture";

   private GlobalConfiguration globalConfiguration;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      BasicComponentRegistry bcr = extractComponent(cr, BasicComponentRegistry.class);
      ChangeDataCaptureConfiguration cdcConfiguration = configuration.module(ChangeDataCaptureConfiguration.class);
      if (cdcConfiguration == null || !cdcConfiguration.enabled())
         return;

      if (!globalConfiguration.features().isAvailable(CDC_FEATURE))
         throw CONFIG.featureDisabled(CDC_FEATURE);

      // Transforms the user-facing configuration into the internal object.
      // This will access the database and validate any connection parameter.
      // The CDC user must also have access to the catalog tables to retrieve constraint information.
      CompleteConfiguration transformed = transformCDCConfiguration(cacheName, cdcConfiguration);

      // TODO: Use the annotations to start the CDC manager.
      //  Must implement https://github.com/infinispan/infinispan/issues/13562 so it start/stop on demand.
      bcr.registerComponent(ChangeDataCaptureManager.class, ChangeDataCaptureManager.create(transformed), true);
      bcr.rewire();
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      ChangeDataCaptureManager cdc = extractComponent(cr, ChangeDataCaptureManager.class);
      if (cdc == null) return;

      cdc.stop();
   }

   @SuppressWarnings("removal")
   private <T> T extractComponent(ComponentRegistry cr, Class<T> clazz) {
      return cr.getComponent(clazz);
   }

   private CompleteConfiguration transformCDCConfiguration(String cacheName, ChangeDataCaptureConfiguration configuration) {
      try {
         return CompleteConfiguration.create(cacheName, configuration);
      } catch (SQLException e) {
         log.errorf(e, "Failed transforming configuration for cache: %s", cacheName);
         throw CONTAINER.invalidDatabaseConfiguration(e);
      }
   }
}
