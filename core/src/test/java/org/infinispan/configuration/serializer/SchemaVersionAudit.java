package org.infinispan.configuration.serializer;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Flattens a published Infinispan configuration JSON schema into the flat set of attribute and
 * element names it declares, ignoring the JSON-Schema structural keywords ({@code properties},
 * {@code $ref}, {@code allOf}, ...) and the PascalCase {@code $defs} type names, keeping only the
 * lowercase kebab-case leaf names that correspond to actual XML attributes and elements.
 */
final class SchemaVersionAudit {

   private static final Set<String> SCHEMA_KEYWORDS = Set.of(
         "$defs", "$ref", "$id", "$schema", "type", "description", "title",
         "required", "properties", "additionalProperties", "items", "enum",
         "allOf", "oneOf", "default", "unevaluatedProperties", "patternProperties");

   private static final Pattern LEAF_NAME = Pattern.compile("^[a-z][a-z0-9-]*$");

   private SchemaVersionAudit() { }

   static List<String> undeclaredSinceViolations(Set<String> knownNames, Set<String> actuallySerializedNames, ConfigurationElement<?> root, Set<String> ignore) {
      List<String> violations = new ArrayList<>();
      collect(knownNames, actuallySerializedNames, root, root.elementName(), violations, ignore);
      return violations;
   }

   private static void collect(Set<String> knownNames, Set<String> actuallySerializedNames, ConfigurationElement<?> element, String path, List<String> violations, Set<String> ignore) {
      for (Attribute<?> attribute : element.attributes().attributes()) {
         AttributeDefinition<?> definition = attribute.getAttributeDefinition();
         String name = path + "." + definition.name();
         if (isViolation(knownNames, actuallySerializedNames, definition.name(), definition.isSince(4, 0))
               && !isIgnored(name, ignore)) {
            violations.add(name);
         }
      }
      for (ConfigurationElement<?> child : element.children()) {
         String childPath = path + "." + child.elementName();
         if (isViolation(knownNames, actuallySerializedNames, child.elementName(), child.isSince(4, 0)) && !isIgnored(childPath, ignore)) {
            violations.add(childPath);
         }
         collect(knownNames, actuallySerializedNames, child, childPath, violations, ignore);
      }
   }

   private static boolean isViolation(Set<String> knownNames, Set<String> actuallySerializedNames, String name, boolean stillAtDefaultSince) {
      return !knownNames.contains(name) && stillAtDefaultSince
            && (actuallySerializedNames == null || actuallySerializedNames.contains(name));
   }

   private static boolean isIgnored(String name, Set<String> ignored) {
      return ignored.stream().anyMatch(name::endsWith);
   }

   static Set<String> namesInXml(String xml) {
      Set<String> names = new HashSet<>();
      XMLInputFactory factory = XMLInputFactory.newFactory();
      try {
         XMLStreamReader reader = factory.createXMLStreamReader(new StringReader(xml));
         while (reader.hasNext()) {
            if (reader.next() == XMLStreamConstants.START_ELEMENT) {
               names.add(reader.getLocalName());
               for (int i = 0; i < reader.getAttributeCount(); i++) {
                  names.add(reader.getAttributeLocalName(i));
               }
            }
         }
      } catch (XMLStreamException e) {
         throw new IllegalArgumentException(e);
      }
      return names;
   }

   static Set<String> knownNames(Json schema) {
      Set<String> names = new HashSet<>();
      collect(schema, names);
      return names;
   }

   private static void collect(Json node, Set<String> names) {
      if (node.isObject()) {
         for (Map.Entry<String, Json> entry : node.asJsonMap().entrySet()) {
            String key = entry.getKey();
            if (!SCHEMA_KEYWORDS.contains(key) && LEAF_NAME.matcher(key).matches()) {
               names.add(key);
            }
            collect(entry.getValue(), names);
         }
      } else if (node.isArray()) {
         for (Json item : node.asJsonList()) {
            collect(item, names);
         }
      }
   }
}
