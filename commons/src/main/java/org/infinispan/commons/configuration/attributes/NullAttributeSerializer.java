package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;

/**
 * @since 10.0
 */
public class NullAttributeSerializer extends AttributeSerializer<Object, ConfigurationInfo, ConfigurationBuilderInfo> {
   public static final AttributeSerializer INSTANCE = new NullAttributeSerializer();

   private NullAttributeSerializer() {
   }

   @Override
   public Object getSerializationValue(Attribute<Object> attribute, ConfigurationInfo configurationElement) {
      return null;
   }
}
