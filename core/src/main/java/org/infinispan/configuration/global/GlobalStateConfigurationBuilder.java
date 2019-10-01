package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalStateConfiguration.ENABLED;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;

/**
 * GlobalStateConfigurationBuilder. Configures filesystem paths where global state is stored.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalStateConfiguration> {
   private final AttributeSet attributes;

   private GlobalStatePathConfigurationBuilder persistentLocation;
   private GlobalStatePathConfigurationBuilder sharedPersistentLocation;
   private TemporaryGlobalStatePathConfigurationBuilder temporaryLocation;
   private GlobalStorageConfigurationBuilder storageConfiguration;

   GlobalStateConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalStateConfiguration.attributeDefinitionSet();
      this.persistentLocation = new GlobalStatePathConfigurationBuilder(globalConfig, Element.PERSISTENT_LOCATION.getLocalName());
      this.sharedPersistentLocation = new GlobalStatePathConfigurationBuilder(globalConfig, Element.SHARED_PERSISTENT_LOCATION.getLocalName());
      this.temporaryLocation = new TemporaryGlobalStatePathConfigurationBuilder(globalConfig);
      this.storageConfiguration = new GlobalStorageConfigurationBuilder(globalConfig);
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
      persistentLocation.location(location, null);
      return this;
   }

   public GlobalStateConfigurationBuilder persistentLocation(String path, String relativeTo) {
      persistentLocation.location(path, relativeTo);
      return this;
   }

   /**
    * Defines the filesystem path where shared persistent state data which needs to survive container restarts
    * should be stored. This path can be safely shared among multiple instances.
    * Defaults to the user.dir system property which usually is where the
    * application was started. This value should be overridden to a more appropriate location.
    */
   public GlobalStateConfigurationBuilder sharedPersistentLocation(String location) {
      sharedPersistentLocation.location(location, null);
      return this;
   }

   public GlobalStateConfigurationBuilder sharedPersistentLocation(String path, String relativeTo) {
      sharedPersistentLocation.location(path, relativeTo);
      return this;
   }

   /**
    * Defines the filesystem path where temporary state should be stored. Defaults to the value of the
    * java.io.tmpdir system property.
    */
   public GlobalStateConfigurationBuilder temporaryLocation(String location) {
      temporaryLocation.location(location, null);
      return this;
   }

   public GlobalStateConfigurationBuilder temporaryLocation(String path, String relativeTo) {
      temporaryLocation.location(path, relativeTo);
      return this;
   }

   /**
    * Defines the {@link ConfigurationStorage} strategy to use. If using {@link ConfigurationStorage#CUSTOM}, then
    * the actual implementation must be passed by invoking {@link #configurationStorageSupplier(Supplier)}
    */
   public GlobalStateConfigurationBuilder configurationStorage(ConfigurationStorage storage) {
      storageConfiguration.configurationStorage(storage);
      return this;
   }

   /**
    * Defines the @{@link LocalConfigurationStorage}. Defaults to @{@link org.infinispan.globalstate.impl.VolatileLocalConfigurationStorage}
    */
   public GlobalStateConfigurationBuilder configurationStorageSupplier(Supplier<? extends LocalConfigurationStorage> configurationStorageSupplier) {
      storageConfiguration.supplier(configurationStorageSupplier);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && persistentLocation.getLocation() == null) {
         CONFIG.missingGlobalStatePersistentLocation();
      }
      storageConfiguration.validate();
   }

   @Override
   public GlobalStateConfiguration create() {
      return new GlobalStateConfiguration(attributes.protect(), persistentLocation.create(), sharedPersistentLocation.create(), temporaryLocation.create(), storageConfiguration.create());
   }

   @Override
   public Builder<?> read(GlobalStateConfiguration template) {
      attributes.read(template.attributes());
      persistentLocation.read(template.persistenceConfiguration());
      sharedPersistentLocation.read(template.sharedPersistenceConfiguration());
      temporaryLocation.read(template.temporaryLocationConfiguration());
      storageConfiguration.read(template.globalStorageConfiguration());
      return this;
   }
}
