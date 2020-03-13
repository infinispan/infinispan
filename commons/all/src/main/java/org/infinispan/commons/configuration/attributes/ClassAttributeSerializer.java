package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.util.Util;

/**
 * Attribute serializer that converters an instance to its class name when serializing and the other way around when deserializing.
 *
 * @since 10.0
 */
public class ClassAttributeSerializer<T, U extends ConfigurationInfo, B extends ConfigurationBuilderInfo> extends AttributeSerializer<T, U, B> {

   public static final AttributeSerializer<Object, ConfigurationInfo, ConfigurationBuilderInfo> INSTANCE = new ClassAttributeSerializer<>();

   @Override
   public Object readAttributeValue(String enclosingElement, AttributeDefinition attributeDefinition, Object attrValue, B builderInfo) {
      return Util.getInstance(attrValue.toString(), builderInfo.getClass().getClassLoader());
   }

}
