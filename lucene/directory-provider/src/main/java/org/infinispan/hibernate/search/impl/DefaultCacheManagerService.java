package org.infinispan.hibernate.search.impl;

import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.engine.service.spi.Startable;
import org.hibernate.search.engine.service.spi.Stoppable;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.util.configuration.impl.ConfigurationParseHelper;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.hibernate.search.logging.Log;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.hibernate.search.util.impl.JNDIHelper;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.kohsuke.MetaInfServices;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;

/**
 * Provides access to Infinispan's CacheManager; one CacheManager is needed for all caches, it can be taken via JNDI or
 * started by this ServiceProvider; in this case it will also be stopped when no longer needed.
 *
 * @author Sanne Grinovero
 */
@MetaInfServices(CacheManagerService.class)
public class DefaultCacheManagerService implements CacheManagerService, Startable, Stoppable {

   private static final Log log = LoggerFactory.make();

   /**
    * If no configuration is defined an no JNDI lookup name is provided, than a new Infinispan CacheManager will be
    * started using this configuration. Such a configuration file is provided in Hibernate Search's jar.
    */
   public static final String DEFAULT_INFINISPAN_CONFIGURATION_RESOURCENAME = "default-hibernatesearch-infinispan.xml";

   /**
    * Reuses the same JNDI name from the second level cache implementation based on Infinispan
    *
    * @see org.hibernate.cache.infinispan.JndiInfinispanRegionFactory.CACHE_MANAGER_RESOURCE_PROP
    */
   public static final String CACHE_MANAGER_RESOURCE_PROP = "hibernate.search.infinispan.cachemanager_jndiname";

   /**
    * The configuration property to use as key to define a custom configuration for Infinispan. Ignored if
    * hibernate.search.infinispan.cachemanager_jndiname is defined.
    */
   public static final String INFINISPAN_CONFIGURATION_RESOURCENAME = "hibernate.search.infinispan.configuration_resourcename";

   /**
    * Configuration property to replace the Transport configuration property in Infinispan with a different value, after
    * the configuration file was parsed.
    */
   public static final String INFINISPAN_TRANSPORT_OVERRIDE_RESOURCENAME = "hibernate.search.infinispan.configuration.transport_override_resourcename";

   private EmbeddedCacheManager cacheManager;

   /**
    * JNDI retrieved cachemanagers are not started by us, so avoid attempting to close them.
    */
   private volatile boolean manageCacheManager = false;

   @Override
   public void start(Properties properties, BuildContext context) {
      ServiceManager serviceManager = context.getServiceManager();
      String name = ConfigurationParseHelper.getString(properties, CACHE_MANAGER_RESOURCE_PROP, null);
      if (name == null) {
         // No JNDI lookup configured: start the CacheManager
         String cfgName = properties.getProperty(
               INFINISPAN_CONFIGURATION_RESOURCENAME,
               DEFAULT_INFINISPAN_CONFIGURATION_RESOURCENAME
         );
         final String transportOverrideResource = properties.getProperty(INFINISPAN_TRANSPORT_OVERRIDE_RESOURCENAME);
         try {
            InfinispanConfigurationParser ispnConfiguration = new InfinispanConfigurationParser();
            ConfigurationBuilderHolder configurationBuilderHolder = ispnConfiguration.parseFile(cfgName, transportOverrideResource, serviceManager);
            cacheManager = new DefaultCacheManager(configurationBuilderHolder, true);
            manageCacheManager = true;
         } catch (IOException e) {
            throw new SearchException(
                  "Could not start Infinispan CacheManager using as configuration file: " + cfgName, e
            );
         }
      } else {
         // use the CacheManager via JNDI
         cacheManager = locateCacheManager(name, JNDIHelper.getJndiProperties(properties, JNDIHelper.HIBERNATE_JNDI_PREFIX));
         manageCacheManager = false;
      }
   }

   private EmbeddedCacheManager locateCacheManager(String jndiNamespace, Properties jndiProperties) {
      Context ctx = null;
      try {
         ctx = new InitialContext(jndiProperties);
         return (EmbeddedCacheManager) ctx.lookup(jndiNamespace);
      } catch (NamingException ne) {
         String msg = "Unable to retrieve CacheManager from JNDI [" + jndiNamespace + "]";
         log.unableToRetrieveCacheManagerFromJndi(jndiNamespace, ne);
         throw new SearchException(msg);
      } finally {
         if (ctx != null) {
            try {
               ctx.close();
            } catch (NamingException ne) {
               log.unableToReleaseInitialContext(ne);
            }
         }
      }
   }

   @Override
   public EmbeddedCacheManager getEmbeddedCacheManager() {
      return cacheManager;
   }

   @Override
   public void stop() {
      if (cacheManager != null && manageCacheManager) {
         cacheManager.stop();
      }
   }
}
