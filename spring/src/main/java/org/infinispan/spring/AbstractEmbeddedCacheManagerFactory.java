package org.infinispan.spring;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
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
      EmbeddedCacheManager cm;
      if (configurationFileLocation != null) {
         return createCacheManager(configurationFileLocation.getInputStream());
      } else {
         if (gcb == null) {
            if (logger.isDebugEnabled()) logger.debug("GlobalConfigurationBuilder is null. Using default new " +
                  "instance.");
            gcb = new GlobalConfigurationBuilder();
         }
         if (builder == null) {
            if (logger.isDebugEnabled()) logger.debug("ConfigurationBuilder is null. Using default new instance.");
            builder = new ConfigurationBuilder();
         }
         cm = createCacheManager(gcb, builder);
         return cm;
      }
   }

   protected EmbeddedCacheManager createCacheManager(InputStream is) throws IOException {
      return new DefaultCacheManager(is);
   }

   protected EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      return new DefaultCacheManager(globalBuilder.build(), builder.build());
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
