package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

class CachedThreadPoolConfiguration extends ConfigurationElement<CachedThreadPoolConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> THREAD_FACTORY = AttributeDefinition.builder(Attribute.THREAD_FACTORY, null, String.class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CachedThreadPoolConfiguration.class, NAME, THREAD_FACTORY);
   }

   CachedThreadPoolConfiguration(AttributeSet attributes) {
      super(Element.CACHED_THREAD_POOL, attributes);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String threadFactory() {
      return attributes.attribute(THREAD_FACTORY).get();
   }
}
