package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;

public class AsElementAttributeSerializer<T, U extends ConfigurationInfo, B extends ConfigurationBuilderInfo> extends AttributeSerializer<T, U, B> {

   @Override
   public String getSerializationName(Attribute<T> attribute, U configurationElement) {
      return null;
   }

   @Override
   public Object getSerializationValue(Attribute<T> attribute, U configurationElement) {
      return null;
   }

   @Override
   public SerializationMode getSerializationMode() {
      return SerializationMode.AS_ELEMENT;
   }
}
