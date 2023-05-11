package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Element;

public class CustomStoreConfiguration extends AbstractStoreConfiguration<CustomStoreConfiguration> {
   public static final AttributeDefinition<Class> CUSTOM_STORE_CLASS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CLASS, null, Class.class).serializer(AttributeSerializer.CLASS_NAME).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CustomStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), CUSTOM_STORE_CLASS);
   }

   private final Attribute<Class> customStoreClass;

   public CustomStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(Element.STORE, attributes, async);
      this.customStoreClass = attributes.attribute(CUSTOM_STORE_CLASS);
   }

   public Class<?> customStoreClass() {
      return customStoreClass.get();
   }

}
