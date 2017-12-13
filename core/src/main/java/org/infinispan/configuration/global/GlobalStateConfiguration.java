package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.globalstate.LocalConfigurationManager;
import org.infinispan.globalstate.impl.EmbeddedLocalConfigurationManager;

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
   public static final AttributeDefinition<String> TEMPORARY_LOCATION = AttributeDefinition
         .builder("temporaryLocation", null, String.class)
            .initializer(() -> SecurityActions.getSystemProperty("java.io.tmpdir")).immutable().build();
   public static final AttributeDefinition<LocalConfigurationManager> LOCAL_CONFIGURATION_MANAGER = AttributeDefinition
         .builder("localConfigurationManager", null, LocalConfigurationManager.class)
         .initializer(EmbeddedLocalConfigurationManager::new).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, ENABLED, PERSISTENT_LOCATION, TEMPORARY_LOCATION, LOCAL_CONFIGURATION_MANAGER);
   }

   private final AttributeSet attributes;
   private final Attribute<Boolean> enabled;
   private Attribute<String> persistentLocation;
   private Attribute<String> temporaryLocation;
   private Attribute<LocalConfigurationManager> localConfigurationManager;

   public GlobalStateConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.persistentLocation = attributes.attribute(PERSISTENT_LOCATION);
      this.temporaryLocation = attributes.attribute(TEMPORARY_LOCATION);
      this.localConfigurationManager = attributes.attribute(LOCAL_CONFIGURATION_MANAGER);
   }

   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Returns the filesystem path where persistent state data which needs to survive container
    * restarts should be stored. Defaults to the user.dir system property which usually is where the
    * application was started.
    */
   public String persistentLocation() {
      return persistentLocation.get();
   }

   /**
    * Returns the filesystem path where temporary state should be stored. Defaults to the value of
    * the java.io.tmpdir system property.
    */
   public String temporaryLocation() {
      return temporaryLocation.get();
   }

   /**
    * Returns the @{@link LocalConfigurationManager}
    */
   public LocalConfigurationManager localConfigurationManager() {
      return localConfigurationManager.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalStateConfiguration [attributes=" + attributes + "]";
   }


}
