package org.infinispan.commons.configuration;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSerializer.SerializationMode;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class JsonReader {

   /**
    * Parses a JSON document into the supplied builder.
    *
    * @param builderInfo The configuration builder to use when reading.
    * @param json the JSON document.
    */
   public void readJson(ConfigurationBuilderInfo builderInfo, String json) {
      readJson(builderInfo, "", Json.read(json));
   }

   private String extractQualifier(Json object) {
      if (object.isArray()) return null;
      if (object.isObject()) {
         Json aClass = object.asJsonMap().get("class");
         if (aClass == null) return null;
         if (!aClass.isObject() && !aClass.isArray()) return aClass.getValue().toString();
      }
      return null;
   }

   private void readJson(ConfigurationBuilderInfo builderInfo, String enclosing, Json json) {
      for (Map.Entry<String, Json> entry : json.asJsonMap().entrySet()) {
         String name = entry.getKey();
         Json value = entry.getValue();
         if (value.isArray()) {
            readArray(builderInfo, enclosing, name, value);
         } else if (value.isObject()) {
            ConfigurationBuilderInfo elementBuilderInfo = builderInfo.getBuilderInfo(name, extractQualifier(value));
            if (elementBuilderInfo != null) {
               readJson(elementBuilderInfo, name, value);
            } else {
               // This could be an attribute serialized as an element
               readNestedAttribute(builderInfo, enclosing, name, value);
            }
         } else {
            readAttribute(builderInfo, enclosing, null, name, value.getValue());
         }
      }
   }

   private void readNestedAttribute(ConfigurationBuilderInfo builderInfo, String element, String nesting, Json value) {
      AttributeSet attributes = builderInfo.attributes();
      if (attributes == null) throw new CacheConfigurationException(
            String.format("Cannot find any attribute or element under '%s' to handle element '%s'", builderInfo, element));
      //Handle attributes serialized as elements
      for (Attribute a : attributes.attributes()) {
         AttributeDefinition<?> attributeDefinition = a.getAttributeDefinition();
         AttributeSerializer<?, ?, ?> serializerConfig = attributeDefinition.getSerializerConfig();
         SerializationMode serializationMode = serializerConfig.getSerializationMode();
         if (serializationMode == SerializationMode.AS_ELEMENT && serializerConfig.canRead(element, nesting, null, attributeDefinition)) {
            readAttribute(builderInfo, element, nesting, null, null);
         }
      }
      // Handle attributes serialized as name/value pair
      Pair simpleAttribute1 = findSimpleAttribute(element, null, nesting, builderInfo);
      if (simpleAttribute1 != null) {
         readAttribute(builderInfo, element, null, nesting, value.getValue());
      }
      // Handle other attributes
      for (Map.Entry<String, Object> entry : value.asMap().entrySet()) {
         String attrName = entry.getKey();
         Object attrValue = entry.getValue();
         Pair simpleAttribute = findSimpleAttribute(element, nesting, attrName, builderInfo);
         if (simpleAttribute != null) {
            readAttribute(builderInfo, element, nesting, attrName, attrValue);
         }
      }
   }

   private void readArray(ConfigurationBuilderInfo builderInfo, String enclosing, String name, Json value) {
      List<Json> elements = value.asJsonList();
      for (Json element : elements) {
         ConfigurationBuilderInfo readerForArray = builderInfo.getNewBuilderInfo(name);
         if (readerForArray != null) {
            readJson(readerForArray, name, element);
         } else {
            Pair pair = findSimpleAttribute(enclosing, null, name, builderInfo);
            if (pair != null) {
               readAttribute(builderInfo, enclosing, null, name, value.getValue());
            } else {
               throw new CacheConfigurationException(String.format("Found multiple '%s' elements under '%s', but cannot find builder info from array under'%s'", name, enclosing, builderInfo));
            }
         }
      }
   }

   private void readAttribute(ConfigurationBuilderInfo builderInfo, String enclosing, String nesting, String name, Object value) {
      Pair pair = findSimpleAttribute(enclosing, nesting, name, builderInfo);
      if (pair != null) {
         readAttribute(enclosing, nesting, value, pair.attribute, pair.builderInfo);
      } else {
         ElementDefinition element = builderInfo.getElementDefinition();
         if (element != null && element.isSynthetic(name)) {
            return;
         }
         throw new CacheConfigurationException(String.format("Could not find attribute definition for '%s' under '%s'", name, builderInfo));
      }
   }

   private void readAttribute(String enclosing, String nesting, Object value, Attribute a, ConfigurationBuilderInfo builderInfo) {
      AttributeDefinition<?> attributeDefinition = a.getAttributeDefinition();
      AttributeSerializer serializerConfig = attributeDefinition.getSerializerConfig();
      Object attrValue = serializerConfig.readAttributeValue(enclosing, nesting, attributeDefinition, value, builderInfo);
      a.set(attrValue);
   }

   private Pair findSimpleAttribute(String enclosing, String nesting, String name, ConfigurationBuilderInfo builderInfo) {
      AttributeSet attributes = builderInfo.attributes();
      if (attributes == null) return null;
      for (Attribute<?> attribute : attributes.attributes()) {
         AttributeDefinition<?> attributeDefinition = attribute.getAttributeDefinition();
         AttributeSerializer<?, ?, ?> serializerConfig = attributeDefinition.getSerializerConfig();
         if (serializerConfig.canRead(enclosing, nesting, name, attributeDefinition)) {
            return new Pair(attribute, builderInfo);
         }
      }
      for (ConfigurationBuilderInfo subReader : builderInfo.getChildrenInfo()) {
         ElementDefinition element = subReader.getElementDefinition();
         if (element != null && !element.isTopLevel()) {
            Pair simpleAttribute = findSimpleAttribute(enclosing, nesting, name, subReader);
            if (simpleAttribute != null) return simpleAttribute;
         }
      }
      return null;
   }

   private static class Pair {
      Attribute<?> attribute;
      ConfigurationBuilderInfo builderInfo;

      Pair(Attribute<?> attribute, ConfigurationBuilderInfo builderInfo) {
         this.attribute = attribute;
         this.builderInfo = builderInfo;
      }
   }

}
