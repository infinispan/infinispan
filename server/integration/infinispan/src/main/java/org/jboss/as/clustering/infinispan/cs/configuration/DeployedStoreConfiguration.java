package org.jboss.as.clustering.infinispan.cs.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

/**
 * Configuration which operates only on class names instead of class objects.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
@BuiltBy(DeployedStoreConfigurationBuilder.class)
public class DeployedStoreConfiguration extends AbstractStoreConfiguration {

   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name" , null, String.class).immutable().build();
   static final AttributeDefinition<String> CUSTOM_STORE_CLASS_NAME =
         AttributeDefinition.builder("customStoreClassName" , null, String.class).immutable().build();


   private PersistenceConfigurationBuilder persistenceConfigurationBuilder;
   private final Attribute<String> name;
   private final Attribute<String> customStoreClassName;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DeployedStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
            NAME, CUSTOM_STORE_CLASS_NAME);
   }

   DeployedStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                     PersistenceConfigurationBuilder persistenceConfigurationBuilder) {
      super(attributes, async, singletonStore);
      this.persistenceConfigurationBuilder = persistenceConfigurationBuilder;
      this.name = attributes.attribute(NAME);
      this.customStoreClassName = attributes.attribute(CUSTOM_STORE_CLASS_NAME);
   }

   public String getName() {
      return name.get();
   }

   public PersistenceConfigurationBuilder getPersistenceConfigurationBuilder() {
      return persistenceConfigurationBuilder;
   }

   public String getCustomStoreClassName() {
      return customStoreClassName.get();
   }
}
