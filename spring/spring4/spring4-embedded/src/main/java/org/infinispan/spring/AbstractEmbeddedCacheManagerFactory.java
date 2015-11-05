package org.infinispan.spring;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.core.io.Resource;

import java.io.IOException;

/**
 * <p>
 * An abstract base class for factories creating cache managers that are backed by an
 * EmbeddedCacheManager.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 */
public class AbstractEmbeddedCacheManagerFactory {

   protected static final Log logger = LogFactory.getLog(AbstractEmbeddedCacheManagerFactory.class);

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
                     .parse(configurationFileLocation.getInputStream());

         if(gcb != null) {
            configurationBuilderHolder.getGlobalConfigurationBuilder().read(gcb.build());
         }
         if (builder != null) {
            configurationBuilderHolder.getDefaultConfigurationBuilder().read(builder.build());
         }

         return new DefaultCacheManager(configurationBuilderHolder, true);
      } else {
         if (gcb == null) {
            if (logger.isDebugEnabled()) logger.debug("GlobalConfigurationBuilder is null. Using default new " +
                                                            "instance.");
            gcb = new GlobalConfigurationBuilder();
            gcb.globalJmxStatistics().allowDuplicateDomains(true);
         }

         if (builder == null) {
            if (logger.isDebugEnabled()) logger.debug("ConfigurationBuilder is null. Using default new instance.");
            builder = new ConfigurationBuilder();
         }

         return new DefaultCacheManager(gcb.build(), builder.build());
      }
   }

   // ------------------------------------------------------------------------
   // Setter for location of configuration file
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link org.springframework.core.io.Resource <code>location</code>} of the
    * configuration file which will be used to configure the
    * {@link org.infinispan.manager.EmbeddedCacheManager <code>EmbeddedCacheManager</code>} the
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager
    * <code>SpringEmbeddedCacheManager</code>} created by this <code>FactoryBean</code> delegates
    * to. If no location is supplied, <tt>Infinispan</tt>'s default configuration will be used.
    * </p>
    * <p>
    * Note that configuration settings defined via using explicit setters exposed by this
    * <code>FactoryBean</code> take precedence over those defined in the configuration file pointed
    * to by <code>configurationFileLocation</code>.
    * </p>
    *
    * @param configurationFileLocation
    *           The {@link org.springframework.core.io.Resource <code>location</code>} of the
    *           configuration file which will be used to configure the
    *           {@link org.infinispan.manager.EmbeddedCacheManager
    *           <code>EmbeddedCacheManager</code>} the
    *           {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager
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
