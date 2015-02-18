package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class CustomStoreConfiguration extends AbstractStoreConfiguration {
   public static final AttributeDefinition<Class> CUSTOM_STORE_CLASS = AttributeDefinition.builder("customStoreClass", null, Class.class).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CustomStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), CUSTOM_STORE_CLASS);
   }

   private final Attribute<Class> customStoreClass;

   public CustomStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      this.customStoreClass = attributes.attribute(CUSTOM_STORE_CLASS);
   }

   public Class<?> customStoreClass() {
      return customStoreClass.get();
   }

}
