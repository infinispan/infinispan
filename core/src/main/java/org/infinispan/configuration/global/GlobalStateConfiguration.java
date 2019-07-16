package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.globalstate.LocalConfigurationStorage;

/**
 *
 * GlobalStateConfiguration.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class GlobalStateConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalStateConfiguration.class, ENABLED);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.GLOBAL_STATE.getLocalName());

   private final AttributeSet attributes;
   private final Attribute<Boolean> enabled;
   private final GlobalStatePathConfiguration persistenceLocationConfiguration;
   private final GlobalStatePathConfiguration sharedPersistenceLocationConfiguration;
   private final TemporaryGlobalStatePathConfiguration temporaryLocationConfiguration;
   private final GlobalStorageConfiguration globalStorageConfiguration;
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   public GlobalStateConfiguration(AttributeSet attributes,
                                   GlobalStatePathConfiguration persistenceLocationConfiguration,
                                   GlobalStatePathConfiguration sharedPersistenceLocationConfiguration,
                                   TemporaryGlobalStatePathConfiguration temporaryLocationConfiguration,
                                   GlobalStorageConfiguration globalStorageConfiguration) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.persistenceLocationConfiguration = persistenceLocationConfiguration;
      this.sharedPersistenceLocationConfiguration = sharedPersistenceLocationConfiguration;
      this.temporaryLocationConfiguration = temporaryLocationConfiguration;
      this.globalStorageConfiguration = globalStorageConfiguration;
      this.subElements.addAll(Arrays.asList(persistenceLocationConfiguration, sharedPersistenceLocationConfiguration, temporaryLocationConfiguration, globalStorageConfiguration));
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
      return persistenceLocationConfiguration.getLocation();
   }


   public GlobalStatePathConfiguration persistenceConfiguration() {
      return persistenceLocationConfiguration;
   }

   /**
    * Returns the filesystem path where shared persistent state data which needs to survive container
    * restarts should be stored. Defaults to the user.dir system property which usually is where the
    * application was started. This path may be shared among multiple instances.
    */
   public String sharedPersistentLocation() {
      return sharedPersistenceLocationConfiguration.getLocation();
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
