package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class BooleanElementConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Boolean> VALUE = AttributeDefinition.builder("value", null, Boolean.class).build();

   private final AttributeSet attributes;
   private final DefaultElementDefinition<ConfigurationInfo> elementDefinition;
   private final String elementName;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(BooleanElementConfiguration.class, VALUE);
   }

   BooleanElementConfiguration(AttributeSet attributes, String elementName) {
      this.attributes = attributes.protect();
      elementDefinition = new DefaultElementDefinition<>(elementName);
      this.elementName = elementName;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   public boolean value() {
      return attributes.attribute(VALUE).get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BooleanElementConfiguration that = (BooleanElementConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      return elementName.equals(that.elementName);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + elementName.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "BooleanElementConfiguration{" +
            "attributes=" + attributes +
            ", elementName='" + elementName + '\'' +
            '}';
   }
}
