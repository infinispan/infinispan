package org.infinispan.commons.configuration;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
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

   private void readJson(ConfigurationBuilderInfo builderInfo, String elementName, Json json) {
      for (Map.Entry<String, Json> entry : json.asJsonMap().entrySet()) {
         String attributeName = entry.getKey();
         Json attributeValue = entry.getValue();
         if (attributeValue.isArray()) {
            readArray(builderInfo, elementName, attributeName, attributeValue);
         } else if (attributeValue.isObject()) {
            ConfigurationBuilderInfo elementBuilderInfo = builderInfo.getBuilderInfo(attributeName, extractQualifier(attributeValue));
            if (elementBuilderInfo != null) {
               readJson(elementBuilderInfo, attributeName, attributeValue);
            } else {
               readAttribute(builderInfo, elementName, attributeName, attributeValue.getValue());
            }
         } else {
            readAttribute(builderInfo, elementName, attributeName, attributeValue.getValue());
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
            Pair pair = findSimpleAttribute(name, builderInfo);
            if (pair != null) {
               readAttribute(builderInfo, enclosing, name, value.getValue());
            } else {
               throw new CacheConfigurationException(String.format("Found multiple '%s' elements under '%s', but cannot find builder info from array under'%s'", name, enclosing, builderInfo));
            }
         }
      }
   }

   private void readAttribute(ConfigurationBuilderInfo builderInfo, String enclosing, String name, Object value) {
      Pair simpleAttribute = findSimpleAttribute(name, builderInfo);
      if (simpleAttribute != null) {
         AttributeDefinition<?> attributeDefinition = ((Attribute) simpleAttribute.attribute).getAttributeDefinition();
         AttributeSerializer serializerConfig = attributeDefinition.getSerializerConfig();
         Object attrValue = serializerConfig.readAttributeValue(enclosing, attributeDefinition, value, simpleAttribute.builderInfo);
         ((Attribute) simpleAttribute.attribute).set(attrValue);
      } else {
         ElementDefinition element = builderInfo.getElementDefinition();
         if (element == null) {
            throw new CacheConfigurationException(String.format("Could not find attribute definition for '%s' under '%s'", name, builderInfo));
         }
      }
   }

   private Pair findSimpleAttribute(String name, ConfigurationBuilderInfo builderInfo) {
      AttributeSet attributes = builderInfo.attributes();
      if (attributes == null) return null;
      for (Attribute<?> attribute : attributes.attributes()) {
         AttributeDefinition<?> attributeDefinition = attribute.getAttributeDefinition();
         AttributeSerializer<?, ?, ?> serializerConfig = attributeDefinition.getSerializerConfig();
         if (serializerConfig.canRead(name, attributeDefinition)) {
            return new Pair(attribute, builderInfo);
         }
      }
      for (ConfigurationBuilderInfo subReader : builderInfo.getChildrenInfo()) {
         ElementDefinition element = subReader.getElementDefinition();
         if (element != null && !element.isTopLevel()) {
            Pair simpleAttribute = findSimpleAttribute(name, subReader);
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
