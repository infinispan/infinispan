package org.jboss.as.clustering.infinispan.cs.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.configuration.cache.*;

import java.util.Properties;

/**
 * Configuration which operates only on class names instead of class objects.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
@BuiltBy(DeployedStoreConfigurationBuilder.class)
public class DeployedStoreConfiguration extends AbstractStoreConfiguration {

   private PersistenceConfigurationBuilder persistenceConfigurationBuilder;
   private String customStoreClassName;

   public DeployedStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                     AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload,
                                     boolean shared, Properties properties, PersistenceConfigurationBuilder persistenceConfigurationBuilder, String customStoreClassName) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.persistenceConfigurationBuilder = persistenceConfigurationBuilder;
      this.customStoreClassName = customStoreClassName;
   }

   public PersistenceConfigurationBuilder getPersistenceConfigurationBuilder() {
      return persistenceConfigurationBuilder;
   }

   public void setPersistenceConfigurationBuilder(PersistenceConfigurationBuilder persistenceConfigurationBuilder) {
      this.persistenceConfigurationBuilder = persistenceConfigurationBuilder;
   }

   public String getCustomStoreClassName() {
      return customStoreClassName;
   }

   public void setCustomStoreClassName(String customStoreClassName) {
      this.customStoreClassName = customStoreClassName;
   }
}
