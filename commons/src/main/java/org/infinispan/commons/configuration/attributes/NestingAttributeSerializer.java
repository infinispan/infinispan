package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * AttributeSerializer for attributes nested into an element that is not related to the {@link ElementDefinition}.
 *
 * @since 10.0
 */
public class NestingAttributeSerializer<T, U extends ConfigurationInfo, B extends ConfigurationBuilderInfo> extends AttributeSerializer<T, U, B> {

   private final String elementName;

   public NestingAttributeSerializer(String elementName) {
      this.elementName = elementName;
   }

   @Override
   public String getParentElement(U configurationElement) {
      return elementName;
   }

   @Override
   public boolean canRead(String enclosing, String nestingName, String nestedName, AttributeDefinition attributeDefinition) {
      return nestingName != null && nestedName != null && nestingName.equals(elementName) && nestedName.equals(attributeDefinition.xmlName());
   }
}
