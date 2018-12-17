package org.infinispan.commons.configuration.attributes;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;

/**
 * Handles {@link Attribute} serialization to external format.
 *
 * @since 10.0
 */
public abstract class AttributeSerializer<T, U extends ConfigurationInfo, B extends ConfigurationBuilderInfo> {

   public enum SerializationMode {AS_ELEMENT, AS_ATTRIBUTE}

   public boolean canRead(String enclosing, String nestingName, String nestedName, AttributeDefinition attributeDefinition) {
      return nestingName == null && nestedName != null && nestedName.equals(attributeDefinition.xmlName());
   }

   public SerializationMode getSerializationMode() {
      return SerializationMode.AS_ATTRIBUTE;
   }


   /**
    * Returns the parent element that this attribute should be placed under when serializing, or empty String if the attribute is not nested.
    */
   public String getParentElement(U configurationElement) {
      return "";
   }

   /**
    * @return The desired serialised attribute name.
    */
   public String getSerializationName(Attribute<T> attribute, U configurationElement) {
      return attribute.getAttributeDefinition().xmlName();
   }

   /**
    * @return The attribute value to be serialized.
    */
   public Object getSerializationValue(Attribute<T> attribute, U configurationElement) {
      return attribute.get();
   }

   /**
    * Read attribute value from serialized format, if {@link #canRead(String, String, String, AttributeDefinition)} is true
    * for this instance of serializer.
    *
    * @param enclosingElement The parent element where the attribute is located.
    * @param nesting In case the attributed is serialized as an element, the name of this element or null otherwise.
    * @param name The serialized attribute name.
    * @param value The serialize attribute value.
    * @param builderInfo the {@link ConfigurationBuilderInfo} where the attribute is defined.
    * @return The attribute value deserialized.
    */
   public Object readAttributeValue(String enclosingElement, String nesting, AttributeDefinition attributeDefinition, Object attrValue, B builderInfo) {
      if (attrValue == null) return null;
      Class type = attributeDefinition.getType();
      AttributeSerializer serializerConfig = attributeDefinition.getSerializerConfig();
      SerializationMode serializationMode = serializerConfig.getSerializationMode();

      boolean isElementSerialization = serializationMode == SerializationMode.AS_ELEMENT;
      if (isElementSerialization) return null;

      if (attrValue instanceof Map && type == TypedProperties.class) {
         TypedProperties typedProperties = new TypedProperties();
         typedProperties.putAll((Map) attrValue);
         return typedProperties;
      } else if (type.isEnum() && !attrValue.getClass().isEnum()) {
         return Enum.valueOf(type, attrValue.toString());
      } else if (type == Set.class) {
         if (attrValue instanceof Collection) {
            return new HashSet<>(((Collection) attrValue));
         }
      } else if (type == Integer.class && attrValue instanceof Number) {
         return ((Number) attrValue).intValue();
      } else if (type == Class.class) {
         return Util.loadClass(attrValue.toString(), builderInfo.getClass().getClassLoader());
      }
      return attrValue;
   }

}
