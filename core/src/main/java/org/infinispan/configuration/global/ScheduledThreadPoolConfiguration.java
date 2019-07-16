package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;

class ScheduledThreadPoolConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> THREAD_FACTORY = AttributeDefinition.builder("threadFactory", null, String.class).build();

   private final AttributeSet attributes;
   private final Attribute<String> name;
   private final Attribute<String> threadFactory;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ScheduledThreadPoolConfiguration.class, NAME, THREAD_FACTORY);
   }

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SCHEDULED_THREAD_POOL.getLocalName());

   ScheduledThreadPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.threadFactory = attributes.attribute(THREAD_FACTORY);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return name.get();
   }

   public String threadFactory() {
      return threadFactory.get();
   }

   @Override
   public String toString() {
      return "ScheduledThreadPoolConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
