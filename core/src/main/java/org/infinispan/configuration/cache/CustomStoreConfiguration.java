package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class CustomStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<Class> CUSTOM_STORE_CLASS = AttributeDefinition.builder("customStoreClass", null, Class.class).immutable().build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(CustomStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), CUSTOM_STORE_CLASS);
   }

   public CustomStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public Class<?> customStoreClass() {
      return attributes.attribute(CUSTOM_STORE_CLASS).asObject(Class.class);
   }

}
