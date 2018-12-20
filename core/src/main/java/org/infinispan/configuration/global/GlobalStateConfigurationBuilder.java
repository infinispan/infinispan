package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalStateConfiguration.CONFIGURATION_STORAGE;
import static org.infinispan.configuration.global.GlobalStateConfiguration.CONFIGURATION_STORAGE_SUPPLIER;
import static org.infinispan.configuration.global.GlobalStateConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalStateConfiguration.PERSISTENT_LOCATION;
import static org.infinispan.configuration.global.GlobalStateConfiguration.SHARED_PERSISTENT_LOCATION;
import static org.infinispan.configuration.global.GlobalStateConfiguration.TEMPORARY_LOCATION;

import java.lang.invoke.MethodHandles;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * GlobalStateConfigurationBuilder. Configures filesystem paths where global state is stored.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateConfigurationBuilder extends AbstractGlobalConfigurationBuilder
      implements Builder<GlobalStateConfiguration> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AttributeSet attributes;

   GlobalStateConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalStateConfiguration.attributeDefinitionSet();
   }

   public GlobalStateConfigurationBuilder enable() {
      return enabled(true);
   }

   public GlobalStateConfigurationBuilder disable() {
      return enabled(false);
   }

   /**
    * Enables or disables the storage of global state.
    */
   public GlobalStateConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   /**
    * Defines the filesystem path where persistent state data which needs to survive container restarts
    * should be stored. The data stored at this location is required for graceful
    * shutdown and restore. This path must NOT be shared among multiple instances.
    * Defaults to the user.dir system property which usually is where the
    * application was started. This value should be overridden to a more appropriate location.
    */
   public GlobalStateConfigurationBuilder persistentLocation(String location) {
      attributes.attribute(PERSISTENT_LOCATION).set(location);
      return this;
   }

   /**
    * Defines the filesystem path where shared persistent state data which needs to survive container restarts
    * should be stored. This path can be safely shared among multiple instances.
    * Defaults to the user.dir system property which usually is where the
    * application was started. This value should be overridden to a more appropriate location.
    */
   public GlobalStateConfigurationBuilder sharedPersistentLocation(String location) {
      attributes.attribute(SHARED_PERSISTENT_LOCATION).set(location);
      return this;
   }

   /**
    * Defines the filesystem path where temporary state should be stored. Defaults to the value of the
    * java.io.tmpdir system property.
    */
   public GlobalStateConfigurationBuilder temporaryLocation(String location) {
      attributes.attribute(TEMPORARY_LOCATION).set(location);
      return this;
   }

   /**
    * Defines the {@link ConfigurationStorage} strategy to use. If using {@link ConfigurationStorage#CUSTOM}, then
    * the actual implementation must be passed by invoking {@link #configurationStorageSupplier(Supplier)}
    */
   public GlobalStateConfigurationBuilder configurationStorage(ConfigurationStorage storage) {
      attributes.attribute(CONFIGURATION_STORAGE).set(storage);
      return this;
   }

   /**
    * Defines the @{@link LocalConfigurationStorage}. Defaults to @{@link org.infinispan.globalstate.impl.VolatileLocalConfigurationStorage}
    */
   public GlobalStateConfigurationBuilder configurationStorageSupplier(Supplier<? extends LocalConfigurationStorage> configurationStorageSupplier) {
      configurationStorage(ConfigurationStorage.CUSTOM);
      attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER).set(configurationStorageSupplier);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && attributes.attribute(PERSISTENT_LOCATION).isNull()) {
         log.missingGlobalStatePersistentLocation();
      }
      if (attributes.attribute(CONFIGURATION_STORAGE).get().equals(ConfigurationStorage.CUSTOM) && attributes.attribute(CONFIGURATION_STORAGE_SUPPLIER).isNull()) {
         throw log.customStorageStrategyNotSet();
      }
   }

   @Override
   public GlobalStateConfiguration create() {
      return new GlobalStateConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(GlobalStateConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
