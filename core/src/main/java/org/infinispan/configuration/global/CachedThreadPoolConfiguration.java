package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

class CachedThreadPoolConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> THREAD_FACTORY = AttributeDefinition.builder("threadFactory", null, String.class).build();

   private final AttributeSet attributes;
   private final Attribute<String> name;
   private final Attribute<String> threadFactory;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CachedThreadPoolConfiguration.class, NAME, THREAD_FACTORY);
   }

   CachedThreadPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.name = attributes.attribute(NAME);
      this.threadFactory = attributes.attribute(THREAD_FACTORY);
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
      return "CachedThreadPoolConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

}
