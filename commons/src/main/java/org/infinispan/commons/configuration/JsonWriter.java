package org.infinispan.commons.configuration;

import java.util.List;
import java.util.Objects;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class JsonWriter {

   static {
      Json.setGlobalFactory(new JsonCustomFactory());
   }

   public String toJSON(ConfigurationInfo configurationInfo) {
      ConfigurationInfo configInfo = Objects.requireNonNull(configurationInfo, "expect a non-null configuration object");
      Json json = Json.object();
      writeElement(json, configInfo, true);
      return json.toString();
   }

   void writeElement(Json parent, ConfigurationInfo element, boolean renderName) {
      ElementDefinition configurationElement = element.getElementDefinition();
      if (configurationElement == null) {
         throw new CacheConfigurationException("No ElementDefinition found for " + this.getClass());
      }

      AttributeSet attributes = element.attributes();
      List<ConfigurationInfo> childElements = element.subElements();

      Json body = Json.object();
      ElementDefinition.ElementOutput elementOutput = element.getElementDefinition().toExternalName(element);

      if (elementOutput.getClassName() != null) {
         body.set("class", elementOutput.getClassName());
      }
      if (attributes != null && !attributes.isEmpty()) {
         writeAttributes(body, attributes, element);
      }

      if (isArray(childElements)) {
         writeArray(body, element, childElements);
      } else {
         for (ConfigurationInfo subElement : childElements) {
            ElementDefinition definition = subElement.getElementDefinition();
            if (definition != null) {
               writeElement(body, subElement, definition.isTopLevel());
            }
         }
      }
      if (!body.asJsonMap().isEmpty()) {
         if (renderName) {
            String name = elementOutput.getName();
            Json existingElement = parent.at(name);
            if (existingElement == null) parent.set(name, Json.object());
            parent.at(name).asJsonMap().putAll(body.asJsonMap());
         } else {
            parent.asJsonMap().putAll(body.asJsonMap());
         }
      }
   }

   private void writeArray(Json parent, ConfigurationInfo configurationInfo, List<ConfigurationInfo> configurationInfos) {
      ElementDefinition.ElementOutput elementOutput = configurationInfos.iterator().next().getElementDefinition().toExternalName(configurationInfo);
      String arrayName = elementOutput.getName();
      Json arrayJson = Json.array();
      configurationInfos.forEach(info -> {
         Json arrayItemJson = Json.object();
         writeElement(arrayItemJson, info, false);
         arrayJson.add(arrayItemJson);
      });
      parent.set(arrayName, arrayJson);
   }

   private void writeAttributes(Json parent, AttributeSet attributeSet, ConfigurationInfo element) {
      Json json = Json.object();

      for (Attribute<?> attribute : attributeSet.attributes()) {
         boolean isPersistent = attribute.isPersistent();
         attribute.getAttributeDefinition().getSerializerConfig();
         AttributeSerializer serializerConfig = attribute.getAttributeDefinition().getSerializerConfig();
         String topLevelElement = serializerConfig.getParentElement(element);
         String attrName = serializerConfig.getSerializationName(attribute, element);
         Object attrValue = serializerConfig.getSerializationValue(attribute, element);

         if (attribute.isModified()) isPersistent = true;

         if (isPersistent && !topLevelElement.isEmpty() && json.at(topLevelElement) == null) {
            json.set(topLevelElement, Json.object());
         }

         if (attribute.isModified()) {
            if (attrName != null && !attrName.isEmpty() && attrValue != null) {
               if (topLevelElement.isEmpty()) {
                  json.set(attrName, attrValue);
               } else {
                  json.at(topLevelElement, Json.object()).set(attrName, attrValue);
               }
            }
         }
      }
      if (json != null) parent.asJsonMap().putAll(json.asJsonMap());
   }

   private boolean isArray(List<ConfigurationInfo> configurationInfos) {
      if (configurationInfos.size() < 2) return false;

      ConfigurationInfo first = configurationInfos.iterator().next();
      ElementDefinition elementDefinition = first.getElementDefinition();
      if (elementDefinition == null) return false;

      String firstElementName = elementDefinition.toExternalName(first).getName();
      return configurationInfos.stream()
            .allMatch(c -> c.getElementDefinition() != null && c.getElementDefinition().toExternalName(c).getName().equals(firstElementName));
   }

}
