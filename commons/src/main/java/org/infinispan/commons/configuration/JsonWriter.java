package org.infinispan.commons.configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
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
         throw new CacheConfigurationException("No ElementDefinition found for " + element.getClass());
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

      Map<String, List<ConfigurationInfo>> elementsByName = groupElementsByName(childElements);
      elementsByName.forEach((name, cfg) -> {
         if (cfg.size() > 1) {
            writeArray(body, element, cfg);
         } else {
            ConfigurationInfo subElement = cfg.iterator().next();
            ElementDefinition definition = subElement.getElementDefinition();
            writeElement(body, subElement, definition.isTopLevel());
         }
      });
      if (!body.asJsonMap().isEmpty() || !configurationElement.omitIfEmpty()) {
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
         AttributeSerializer serializerConfig = attribute.getAttributeDefinition().getSerializerConfig();
         String attrName = serializerConfig.getSerializationName(attribute, element);
         Object attrValue = serializerConfig.getSerializationValue(attribute, element);
         if (attribute.isModified()) {
            if (attrName == null && attrValue instanceof Map) {
               Map<String, Object> valueMap = (Map<String, Object>) attrValue;
               valueMap.forEach(json::set);
            } else {
               if (attrName != null && !attrName.isEmpty() && attrValue != null) {
                  json.set(attrName, attrValue);
               }
            }
         }
      }
      if (json != null) parent.asJsonMap().putAll(json.asJsonMap());
   }

   private Map<String, List<ConfigurationInfo>> groupElementsByName(List<ConfigurationInfo> configurationInfos) {
      Map<String, List<ConfigurationInfo>> configsByName = new LinkedHashMap<>();
      configurationInfos.forEach(c -> {
         ElementDefinition elementDefinition = c.getElementDefinition();
         if (elementDefinition != null) {
            String elementName = elementDefinition.toExternalName(c).getName();
            configsByName.computeIfAbsent(elementName, v -> new ArrayList<>()).add(c);
         }
      });
      return configsByName;
   }

}
