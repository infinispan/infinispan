package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalStateConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalStateConfiguration.UNCLEAN_SHUTDOWN_ACTION;
import static org.infinispan.configuration.global.GlobalStatePathConfiguration.PATH;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.function.Supplier;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
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

   private final GlobalStatePathConfigurationBuilder persistentLocation;
   private final GlobalStatePathConfigurationBuilder sharedPersistentLocation;
   private final TemporaryGlobalStatePathConfigurationBuilder temporaryLocation;
   private final GlobalStorageConfigurationBuilder storageConfiguration;

   GlobalStateConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalStateConfiguration.attributeDefinitionSet();
      this.persistentLocation = new GlobalStatePathConfigurationBuilder(globalConfig, Element.PERSISTENT_LOCATION.getLocalName());
      this.sharedPersistentLocation = new GlobalStatePathConfigurationBuilder(globalConfig, Element.SHARED_PERSISTENT_LOCATION.getLocalName());
      this.temporaryLocation = new TemporaryGlobalStatePathConfigurationBuilder(globalConfig);
      this.storageConfiguration = new GlobalStorageConfigurationBuilder(globalConfig);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
    * Defines the action taken when a dangling lock file is found in the persistent global state, signifying an
    * unclean shutdown of the node (usually because of a crash or an external termination).
    */
   public GlobalStateConfigurationBuilder uncleanShutdownAction(UncleanShutdownAction action) {
      attributes.attribute(UNCLEAN_SHUTDOWN_ACTION).set(action);
      return this;
   }

   /**
    * Defines the filesystem path where node-specific persistent data which needs to survive container restarts
    * should be stored. This path will be used as the default storage location for file-based cache stores such as
    * the default {@link org.infinispan.persistence.sifs.NonBlockingSoftIndexFileStore} as well as the consistent hash for all caches
    * which enables graceful shutdown and restart. Because the data stored in the persistent
    * location is specific to the node that owns it, this path <b>MUST NOT</b> be shared among multiple instances.
    * Defaults to the user.dir system property which usually is where the application was started.
    * This value should be overridden to a more appropriate location.
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
    * should be stored. In particular this path will contain persistent dynamic configuration, as that managed by the
    * {@link org.infinispan.globalstate.impl.OverlayLocalConfigurationStorage}.
    * This path <b>MAY</b> be safely shared among multiple instances.
    * If not set, this will use the {@link #persistentLocation} value.
    * This value should be overridden to a more appropriate location.
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
      if (!attributes.attribute(ENABLED).get() && persistentLocation.attributes().attribute(PATH).isModified()) {
         throw new CacheConfigurationException("GlobalState persistent location set, but state not enabled");
      }
      storageConfiguration.validate();
   }

   @Override
   public GlobalStateConfiguration create() {
      return new GlobalStateConfiguration(attributes.protect(), persistentLocation.create(), sharedPersistentLocation.create(), temporaryLocation.create(), storageConfiguration.create());
   }

   @Override
   public Builder<?> read(GlobalStateConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      persistentLocation.read(template.persistenceConfiguration(), combine);
      sharedPersistentLocation.read(template.sharedPersistenceConfiguration(), combine);
      temporaryLocation.read(template.temporaryLocationConfiguration(), combine);
      storageConfiguration.read(template.globalStorageConfiguration(), combine);
      return this;
   }
}
