package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;

/**
 * Default implementation of {@link AttributeSerializer} that uses the xmlName as the output format for the name,
 * and the value without transformation.
 */
public class DefaultSerializer<T, U extends ConfigurationInfo, B extends ConfigurationBuilderInfo> extends AttributeSerializer<T, U, B> {

   private final String attributeName;

   DefaultSerializer(String attributeName) {
      this.attributeName = attributeName;
   }

   @Override
   public String getSerializationName(Attribute<T> attribute, U configurationElement) {
      return attributeName == null ? attribute.getAttributeDefinition().xmlName() : attributeName;
   }

}
