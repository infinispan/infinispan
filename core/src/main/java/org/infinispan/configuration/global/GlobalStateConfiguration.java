package org.infinispan.configuration.global;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable()
         .build();
   public static final AttributeDefinition<String> PERSISTENT_LOCATION = AttributeDefinition
         .builder("persistentLocation", null, String.class)
            .initializer(() -> SecurityActions.getSystemProperty("user.dir")).immutable().build();
   public static final AttributeDefinition<String> SHARED_PERSISTENT_LOCATION = AttributeDefinition
         .builder("sharedPersistentLocation", null, String.class)
         .initializer(() -> SecurityActions.getSystemProperty("user.dir")).immutable().build();
   public static final AttributeDefinition<String> TEMPORARY_LOCATION = AttributeDefinition
         .builder("temporaryLocation", null, String.class)
            .initializer(() -> SecurityActions.getSystemProperty("java.io.tmpdir")).immutable().build();
   public static final AttributeDefinition<ConfigurationStorage> CONFIGURATION_STORAGE = AttributeDefinition
         .builder("configurationStorage", ConfigurationStorage.VOLATILE, ConfigurationStorage.class).autoPersist(false)
         .immutable().build();
   public static final AttributeDefinition<Supplier<? extends LocalConfigurationStorage>> CONFIGURATION_STORAGE_SUPPLIER = AttributeDefinition
         .supplierBuilder("configurationStorageSupplier", LocalConfigurationStorage.class).autoPersist(false)
         .immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, ENABLED, PERSISTENT_LOCATION, SHARED_PERSISTENT_LOCATION, TEMPORARY_LOCATION, CONFIGURATION_STORAGE, CONFIGURATION_STORAGE_SUPPLIER);
   }

   private final AttributeSet attributes;
   private final Attribute<Boolean> enabled;
   private final Attribute<String> persistentLocation;
   private Attribute<String> sharedPersistentLocation;
   private final Attribute<String> temporaryLocation;
   private final Attribute<ConfigurationStorage> configurationStorage;
   private final Attribute<Supplier<? extends LocalConfigurationStorage>> configurationStorageSupplier;

   public GlobalStateConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.persistentLocation = attributes.attribute(PERSISTENT_LOCATION);
      this.sharedPersistentLocation = attributes.attribute(SHARED_PERSISTENT_LOCATION);
      this.temporaryLocation = attributes.attribute(TEMPORARY_LOCATION);
      this.configurationStorage = attributes.attribute(CONFIGURATION_STORAGE);
      this.configurationStorageSupplier = attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER);
   }

   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Returns the filesystem path where persistent state data which needs to survive container
    * restarts should be stored. Defaults to the user.dir system property which usually is where the
    * application was started. Warning: this path must NOT be shared with other instances.
    */
   public String persistentLocation() {
      return persistentLocation.get();
   }

   /**
    * Returns the filesystem path where shared persistent state data which needs to survive container
    * restarts should be stored. Defaults to the user.dir system property which usually is where the
    * application was started. This path may be shared among multiple instances.
    */
   public String sharedPersistentLocation() {
      return sharedPersistentLocation.get();
   }

   /**
    * Returns the filesystem path where temporary state should be stored. Defaults to the value of
    * the java.io.tmpdir system property.
    */
   public String temporaryLocation() {
      return temporaryLocation.get();
   }

   public ConfigurationStorage configurationStorage() {
      return configurationStorage.get();
   }

   /**
    * Returns the {@link LocalConfigurationStorage} {@link Supplier}
    */
   public Supplier<? extends LocalConfigurationStorage> configurationStorageClass() {
      return configurationStorageSupplier.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalStateConfiguration [attributes=" + attributes + "]";
   }
}
