package org.infinispan.configuration.global;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;

/**
 *
 * GlobalStateConfiguration.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(Attribute.ENABLED, false).autoPersist(false).immutable().build();
   public static final AttributeDefinition<UncleanShutdownAction> UNCLEAN_SHUTDOWN_ACTION = AttributeDefinition.builder(Attribute.UNCLEAN_SHUTDOWN_ACTION, UncleanShutdownAction.FAIL).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, ENABLED, UNCLEAN_SHUTDOWN_ACTION);
   }

   private final AttributeSet attributes;
   private final boolean enabled;
   private final GlobalStatePathConfiguration persistenceLocationConfiguration;
   private final GlobalStatePathConfiguration sharedPersistenceLocationConfiguration;
   private final TemporaryGlobalStatePathConfiguration temporaryLocationConfiguration;
   private final GlobalStorageConfiguration globalStorageConfiguration;

   public GlobalStateConfiguration(AttributeSet attributes,
                                   GlobalStatePathConfiguration persistenceLocationConfiguration,
                                   GlobalStatePathConfiguration sharedPersistenceLocationConfiguration,
                                   TemporaryGlobalStatePathConfiguration temporaryLocationConfiguration,
                                   GlobalStorageConfiguration globalStorageConfiguration) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED).get();
      this.persistenceLocationConfiguration = persistenceLocationConfiguration;
      this.sharedPersistenceLocationConfiguration = sharedPersistenceLocationConfiguration;
      this.temporaryLocationConfiguration = temporaryLocationConfiguration;
      this.globalStorageConfiguration = globalStorageConfiguration;
   }

   public boolean enabled() {
      return enabled;
   }

   public UncleanShutdownAction uncleanShutdownAction() {
      return attributes.attribute(UNCLEAN_SHUTDOWN_ACTION).get();
   }

   /**
    * Returns the filesystem path where persistent state data which needs to survive container
    * restarts should be stored. Defaults to the user.dir system property which usually is where the
    * application was started. Warning: this path must NOT be shared with other instances.
    */
   public String persistentLocation() {
      return persistenceLocationConfiguration.getLocation();
   }


   public GlobalStatePathConfiguration persistenceConfiguration() {
      return persistenceLocationConfiguration;
   }

   /**
    * Returns the filesystem path where shared persistent state data which needs to survive container
    * restarts should be stored. Defaults to the {@link #persistentLocation()}.
    * This path may be shared among multiple instances.
    */
   public String sharedPersistentLocation() {
      if (sharedPersistenceLocationConfiguration.isModified()) {
         return sharedPersistenceLocationConfiguration.getLocation();
      } else {
         return persistentLocation();
      }
   }

   public GlobalStatePathConfiguration sharedPersistenceConfiguration() {
      return sharedPersistenceLocationConfiguration;
   }

   /**
    * Returns the filesystem path where temporary state should be stored. Defaults to the value of
    * the java.io.tmpdir system property.
    */
   public String temporaryLocation() {
      return temporaryLocationConfiguration.getLocation();
   }

   public TemporaryGlobalStatePathConfiguration temporaryLocationConfiguration() {
      return temporaryLocationConfiguration;
   }

   public ConfigurationStorage configurationStorage() {
      return globalStorageConfiguration.configurationStorage();
   }

   public GlobalStorageConfiguration globalStorageConfiguration() {
      return globalStorageConfiguration;
   }

   /**
    * Returns the {@link LocalConfigurationStorage} {@link Supplier}
    */
   public Supplier<? extends LocalConfigurationStorage> configurationStorageClass() {
      return globalStorageConfiguration.storageSupplier();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalStateConfiguration [attributes=" + attributes + "]";
   }
}
