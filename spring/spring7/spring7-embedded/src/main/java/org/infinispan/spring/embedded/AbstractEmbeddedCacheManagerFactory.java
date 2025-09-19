package org.infinispan.spring.embedded;

import java.io.IOException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.core.io.Resource;

/**
 * <p>
 * An abstract base class for factories creating cache managers that are backed by an
 * EmbeddedCacheManager.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 */
public class AbstractEmbeddedCacheManagerFactory {
   private static final Log logger = LogFactory.getLog(AbstractEmbeddedCacheManagerFactory.class);

   private Resource configurationFileLocation;
   private GlobalConfigurationBuilder gcb;
   private ConfigurationBuilder builder;

   // ------------------------------------------------------------------------
   // Create fully configured EmbeddedCacheManager instance
   // ------------------------------------------------------------------------

   protected EmbeddedCacheManager createBackingEmbeddedCacheManager() throws IOException {
      if (configurationFileLocation != null) {
         ConfigurationBuilderHolder configurationBuilderHolder =
               new ParserRegistry(Thread.currentThread().getContextClassLoader())
                     .parse(configurationFileLocation.getURL());

         if(gcb != null) {
            configurationBuilderHolder.getGlobalConfigurationBuilder().read(gcb.build());
         }
         if (builder != null) {
            ConfigurationBuilder dcb = configurationBuilderHolder.getDefaultConfigurationBuilder();
            if (dcb != null)
               dcb.read(builder.build());
            else
               throw logger.noDefaultCache();
         }

         return new DefaultCacheManager(configurationBuilderHolder, true);
      } else {
         if (gcb == null) {
            if (logger.isDebugEnabled()) logger.debug("GlobalConfigurationBuilder is null. Using default new " +
                                                            "instance.");
            gcb = new GlobalConfigurationBuilder();
         }

         if (builder != null) {
            ConfigurationBuilderHolder configurationBuilderHolder =
                  new ConfigurationBuilderHolder(Thread.currentThread().getContextClassLoader(), gcb);
            configurationBuilderHolder.getGlobalConfigurationBuilder().read(gcb.build());
            if (gcb.defaultCacheName().isPresent()) {
               configurationBuilderHolder.getNamedConfigurationBuilders().put(gcb.defaultCacheName().get(), builder);
            } else {
               throw logger.noDefaultCache();
            }
            return new DefaultCacheManager(configurationBuilderHolder, true);
         } else {
            return new DefaultCacheManager(gcb.build());
         }
      }
   }

   // ------------------------------------------------------------------------
   // Setter for location of configuration file
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link Resource <code>location</code>} of the
    * configuration file which will be used to configure the
    * {@link EmbeddedCacheManager <code>EmbeddedCacheManager</code>} the
    * {@link org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager
    * <code>SpringEmbeddedCacheManager</code>} created by this <code>FactoryBean</code> delegates
    * to. If no location is supplied, <code>Infinispan</code>'s default configuration will be used.
    * </p>
    * <p>
    * Note that configuration settings defined via using explicit setters exposed by this
    * <code>FactoryBean</code> take precedence over those defined in the configuration file pointed
    * to by <code>configurationFileLocation</code>.
    * </p>
    *
    * @param configurationFileLocation
    *           The {@link Resource <code>location</code>} of the
    *           configuration file which will be used to configure the
    *           {@link EmbeddedCacheManager
    *           <code>EmbeddedCacheManager</code>} the
    *           {@link org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager
    *           <code>SpringEmbeddedCacheManager</code>} created by this <code>FactoryBean</code>
    *           delegates to
    */
   public void setConfigurationFileLocation(final Resource configurationFileLocation) {
      this.configurationFileLocation = configurationFileLocation;
   }

   /**
    * Sets the {@link GlobalConfigurationBuilder} to use when creating an <code>EmbeddedCacheManager</code>.
    *
    * @param gcb the <code>GlobalConfigurationBuilder</code> instance.
    */
   public void addCustomGlobalConfiguration(final GlobalConfigurationBuilder gcb) {
      this.gcb = gcb;
   }

   /**
    * Sets the {@link ConfigurationBuilder} to use when creating an <code>EmbeddedCacheManager</code>.
    *
    * @param builder the <code>ConfigurationBuilder</code> instance.
    */
   public void addCustomCacheConfiguration(final ConfigurationBuilder builder) {
      this.builder = builder;
   }

}
