package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalStateConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalStateConfiguration.LOCAL_CONFIGURATION_MANAGER;
import static org.infinispan.configuration.global.GlobalStateConfiguration.PERSISTENT_LOCATION;
import static org.infinispan.configuration.global.GlobalStateConfiguration.TEMPORARY_LOCATION;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.globalstate.LocalConfigurationManager;
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
      shutdown and restore. Defaults to the user.dir system property which usually is where the
    * application was started. This value should be overridden to a more appropriate location.
    */
   public GlobalStateConfigurationBuilder persistentLocation(String location) {
      attributes.attribute(PERSISTENT_LOCATION).set(location);
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
    * Defines the @{@link LocalConfigurationManager}. Defaults to @{@link org.infinispan.globalstate.impl.EmbeddedLocalConfigurationManager}
    */
   public GlobalStateConfigurationBuilder localConfigurationManager(LocalConfigurationManager localConfigurationManager) {
      attributes.attribute(LOCAL_CONFIGURATION_MANAGER).set(localConfigurationManager);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && attributes.attribute(PERSISTENT_LOCATION).isNull()) {
         log.missingGlobalStatePersistentLocation();
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
