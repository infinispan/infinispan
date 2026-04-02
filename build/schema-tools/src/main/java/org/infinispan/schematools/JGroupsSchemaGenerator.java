package org.infinispan.schematools;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

/**
 * Generates a JSON Schema file containing definitions for all JGroups protocols
 * and their {@link Property}-annotated attributes.
 * <p>
 * This mirrors {@link org.jgroups.util.XMLSchemaGenerator} but produces JSON Schema
 * (Draft 2020-12) output instead of XSD.
 *
 * @since 16.2
 */
public class JGroupsSchemaGenerator {

   private static final String PROT_PACKAGE = "org.jgroups.protocols";
   private static final String[] PACKAGES = {"", "pbcast", "relay", "dns"};

   public static void main(String[] args) throws Exception {
      String outputFile = "jgroups-protocols.json";
      for (int i = 0; i < args.length; i++) {
         if ("-o".equals(args[i])) {
            outputFile = args[++i];
         }
      }

      Map<String, Object> schema = new LinkedHashMap<>();
      schema.put("$id", "https://infinispan.org/jgroups-config");
      schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
      schema.put("title", "JGroups");
      schema.put("description", "JGroups transport stack configuration for Infinispan.");
      schema.put("type", "object");

      Map<String, Object> defs = new LinkedHashMap<>();
      schema.put("$defs", defs);

      // Generate protocol definitions
      Map<String, Object> protocolDefs = new TreeMap<>();
      for (String suffix : PACKAGES) {
         String packageName = PROT_PACKAGE + (suffix.isEmpty() ? "" : "." + suffix);
         Set<Class<?>> classes = getProtocolClasses(packageName);
         List<Class<?>> sorted = classes.stream()
               .sorted(Comparator.comparing(Class::getSimpleName))
               .toList();
         for (Class<?> clazz : sorted) {
            String protocolName = protocolElementName(clazz, packageName);
            Map<String, Object> protocolSchema = generateProtocolSchema(clazz);
            protocolDefs.put(protocolName, protocolSchema);
         }
      }

      // Build the JGroups top-level def
      defs.put("JGroups", buildJGroupsDef());
      defs.put("JGroupsStackDefinition", buildStackDefinitionDef());
      defs.put("JGroupsStackFile", buildStackFileDef());
      defs.put("JGroupsStack", buildStackDef(protocolDefs));
      defs.put("JGroupsRemoteSites", buildRemoteSitesDef());
      defs.put("JGroupsRemoteSite", buildRemoteSiteDef());

      // Add each protocol as a separate $def
      defs.putAll(protocolDefs);

      try (FileWriter fw = new FileWriter(outputFile)) {
         fw.write(toJson(schema, 0));
         fw.write("\n");
      }

      System.out.printf("Generated JGroups JSON Schema with %d protocol definitions to %s%n",
            protocolDefs.size(), outputFile);
   }

   private static String protocolElementName(Class<?> clazz, String packageName) {
      String name = packageName + "." + clazz.getSimpleName();
      return name.replace(PROT_PACKAGE + ".", "");
   }

   private static Set<Class<?>> getProtocolClasses(String packageName) {
      Set<Class<?>> classes = new HashSet<>();
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      String path = packageName.replace('.', '/');
      try {
         Enumeration<URL> resources = loader.getResources(path);
         while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            if ("file".equals(resource.getProtocol())) {
               java.io.File dir = new java.io.File(resource.getFile());
               if (dir.isDirectory()) {
                  for (String file : dir.list()) {
                     if (file.endsWith(".class") && !file.contains("$")) {
                        addClass(classes, packageName + '.' + file.replace(".class", ""));
                     }
                  }
               }
            } else if ("jar".equals(resource.getProtocol())) {
               JarURLConnection conn = (JarURLConnection) resource.openConnection();
               try (JarFile jar = conn.getJarFile()) {
                  String prefix = path + "/";
                  Enumeration<JarEntry> entries = jar.entries();
                  while (entries.hasMoreElements()) {
                     String entryName = entries.nextElement().getName();
                     if (entryName.startsWith(prefix) && entryName.endsWith(".class")
                           && !entryName.contains("$")) {
                        // Only direct children (no sub-packages)
                        String relative = entryName.substring(prefix.length());
                        if (!relative.contains("/")) {
                           addClass(classes, entryName.replace('/', '.').replace(".class", ""));
                        }
                     }
                  }
               }
            }
         }
      } catch (IOException e) {
         // skip
      }
      return classes;
   }

   private static void addClass(Set<Class<?>> classes, String className) {
      try {
         Class<?> clazz = Class.forName(className);
         int mods = clazz.getModifiers();
         if (Modifier.isPublic(mods) && !Modifier.isAbstract(mods)
               && !clazz.isAnonymousClass()
               && Protocol.class.isAssignableFrom(clazz)) {
            classes.add(clazz);
         }
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
         // skip
      }
   }

   private static Map<String, Object> generateProtocolSchema(Class<?> clazz) {
      Map<String, Object> schema = new LinkedHashMap<>();
      schema.put("type", "object");
      schema.put("description", "Protocol " + clazz.getSimpleName());

      Map<String, Object> properties = new TreeMap<>();

      // Collect properties from fields
      collectFieldProperties(clazz, properties, null);

      // Collect properties from @Component fields
      Util.forAllComponentTypes(clazz, (componentClass, prefix) -> {
         collectFieldProperties(componentClass, properties, prefix);
         if (componentClass.isInterface()) {
            try {
               String pkg = componentClass.getPackageName();
               Set<Class<?>> impls = getProtocolClasses(pkg);
               for (Class<?> impl : impls) {
                  if (componentClass.isAssignableFrom(impl)) {
                     collectFieldProperties(impl, properties, prefix);
                     collectMethodProperties(impl, properties, prefix);
                  }
               }
            } catch (Exception e) {
               // skip
            }
         }
         collectMethodProperties(componentClass, properties, prefix);
      });

      // Collect properties from methods
      collectMethodProperties(clazz, properties, null);

      if (!properties.isEmpty()) {
         schema.put("properties", properties);
      }
      schema.put("additionalProperties", Map.of("type", "string"));
      return schema;
   }

   private static void collectFieldProperties(Class<?> clazz, Map<String, Object> properties, String prefix) {
      Field[] fields = Util.getAllDeclaredFieldsWithAnnotations(clazz, Property.class);
      for (Field field : fields) {
         Property prop = field.getAnnotation(Property.class);
         if (!prop.deprecatedMessage().isEmpty()) continue;

         boolean annotationRedefinesName = !prop.name().isEmpty();
         String attrName = annotationRedefinesName ? prop.name() : field.getName();
         String qualifiedName = prefix != null && !prefix.trim().isEmpty()
               ? prefix + "." + attrName : attrName;

         Map<String, Object> propSchema = new LinkedHashMap<>();
         propSchema.put("type", "string");
         if (!prop.description().isEmpty()) {
            propSchema.put("description", prop.description());
         }
         properties.put(qualifiedName, propSchema);
      }
   }

   private static void collectMethodProperties(Class<?> clazz, Map<String, Object> properties, String prefix) {
      for (Method method : clazz.getMethods()) {
         if (method.isAnnotationPresent(Property.class)) {
            Property prop = method.getAnnotation(Property.class);
            if (!prop.deprecatedMessage().isEmpty()) continue;

            String name = prop.name().isEmpty()
                  ? Util.methodNameToAttributeName(method.getName()) : prop.name();
            String qualifiedName = prefix != null && !prefix.trim().isEmpty()
                  ? prefix + "." + name : name;

            Map<String, Object> propSchema = new LinkedHashMap<>();
            propSchema.put("type", "string");
            if (!prop.description().isEmpty()) {
               propSchema.put("description", prop.description());
            }
            properties.put(qualifiedName, propSchema);
         }
      }
   }

   // --- Static schema structure builders ---

   private static Map<String, Object> buildJGroupsDef() {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("title", "JGroups");
      def.put("description", "Defines JGroups transport stacks.");
      def.put("additionalProperties", false);
      Map<String, Object> props = new LinkedHashMap<>();
      props.put("transport", Map.of("type", "string",
            "description", "Class that represents a network transport. Must implement org.infinispan.remoting.transport.Transport."));
      props.put("stacks", Map.of("type", "object",
            "description", "Named JGroups stacks.",
            "additionalProperties", Map.of("$ref", "#/$defs/JGroupsStackDefinition")));
      def.put("properties", props);
      return def;
   }

   private static Map<String, Object> buildStackDefinitionDef() {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("description", "Defines an individual JGroups stack, either by referencing a file or inline protocol configuration.");
      Map<String, Object> props = new LinkedHashMap<>();
      props.put("stack-file", Map.of("$ref", "#/$defs/JGroupsStackFile"));
      props.put("stack", Map.of("$ref", "#/$defs/JGroupsStack"));
      def.put("properties", props);
      def.put("additionalProperties", false);
      return def;
   }

   private static Map<String, Object> buildStackFileDef() {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("description", "Defines a JGroups stack by pointing to a file containing its definition.");
      Map<String, Object> props = new LinkedHashMap<>();
      props.put("path", Map.of("type", "string",
            "description", "Path of JGroups configuration file containing stack definition."));
      def.put("properties", props);
      def.put("additionalProperties", false);
      return def;
   }

   private static Map<String, Object> buildStackDef(Map<String, Object> protocolDefs) {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("description", "Defines a JGroups stack inline with protocol configurations. Protocol names are keys, their properties are values (object or null).");

      Map<String, Object> props = new LinkedHashMap<>();
      props.put("extends", Map.of("type", "string",
            "description", "The base stack to extend."));
      props.put("remote-sites", Map.of("$ref", "#/$defs/JGroupsRemoteSites"));

      // Add each known protocol as a named property with $ref
      for (String protocolName : protocolDefs.keySet()) {
         Map<String, Object> protocolProp = new LinkedHashMap<>();
         protocolProp.put("oneOf", List.of(
               Map.of("type", "null"),
               Map.of("$ref", "#/$defs/" + protocolName)
         ));
         props.put(protocolName, protocolProp);
      }

      def.put("properties", props);
      // Still allow unknown protocols (custom or from other packages)
      def.put("additionalProperties", Map.of(
            "description", "A JGroups protocol. The key is the protocol name. The value is either null (use defaults) or an object with protocol properties.",
            "oneOf", List.of(
                  Map.of("type", "null"),
                  Map.of("type", "object", "additionalProperties", Map.of("type", "string"))
            )
      ));
      return def;
   }

   private static Map<String, Object> buildRemoteSitesDef() {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("description", "Defines the relay configuration for cross-site replication.");
      Map<String, Object> props = new LinkedHashMap<>();
      props.put("default-stack", Map.of("type", "string",
            "description", "Defines the name of the JGroups stack to be used by default when connecting to remote sites."));
      props.put("cluster", Map.of("type", "string",
            "description", "Defines the default cluster name for remote clusters."));
      Map<String, Object> remoteSite = new LinkedHashMap<>();
      remoteSite.put("oneOf", List.of(
            Map.of("$ref", "#/$defs/JGroupsRemoteSite"),
            Map.of("type", "array", "items", Map.of("$ref", "#/$defs/JGroupsRemoteSite"))
      ));
      remoteSite.put("description", "Defines remote site(s).");
      props.put("remote-site", remoteSite);
      def.put("properties", props);
      def.put("additionalProperties", false);
      return def;
   }

   private static Map<String, Object> buildRemoteSiteDef() {
      Map<String, Object> def = new LinkedHashMap<>();
      def.put("type", "object");
      def.put("description", "Defines a remote site for cross-site replication.");
      Map<String, Object> props = new LinkedHashMap<>();
      props.put("name", Map.of("type", "string",
            "description", "Defines the name of the remote site."));
      props.put("stack", Map.of("type", "string",
            "description", "Defines the name of the JGroups stack to use to connect to the remote site."));
      props.put("cluster", Map.of("type", "string",
            "description", "Defines the name for the underlying group communication cluster."));
      def.put("properties", props);
      def.put("required", List.of("name"));
      def.put("additionalProperties", false);
      return def;
   }

   // --- JSON serialization (no external dependency) ---

   private static String toJson(Object obj, int indent) {
      StringBuilder sb = new StringBuilder();
      writeJson(sb, obj, indent);
      return sb.toString();
   }

   @SuppressWarnings("unchecked")
   private static void writeJson(StringBuilder sb, Object obj, int indent) {
      if (obj == null) {
         sb.append("null");
      } else if (obj instanceof Map) {
         writeJsonMap(sb, (Map<String, Object>) obj, indent);
      } else if (obj instanceof List) {
         writeJsonList(sb, (List<Object>) obj, indent);
      } else if (obj instanceof Boolean) {
         sb.append(obj);
      } else if (obj instanceof Number) {
         sb.append(obj);
      } else {
         sb.append('"').append(escapeJson(obj.toString())).append('"');
      }
   }

   private static void writeJsonMap(StringBuilder sb, Map<String, Object> map, int indent) {
      if (map.isEmpty()) {
         sb.append("{}");
         return;
      }
      sb.append("{\n");
      var entries = map.entrySet().iterator();
      while (entries.hasNext()) {
         var entry = entries.next();
         sb.append(spaces(indent + 2));
         sb.append('"').append(escapeJson(entry.getKey())).append("\": ");
         writeJson(sb, entry.getValue(), indent + 2);
         if (entries.hasNext()) sb.append(',');
         sb.append('\n');
      }
      sb.append(spaces(indent)).append('}');
   }

   @SuppressWarnings("unchecked")
   private static void writeJsonList(StringBuilder sb, List<Object> list, int indent) {
      if (list.isEmpty()) {
         sb.append("[]");
         return;
      }
      sb.append("[\n");
      var it = list.iterator();
      while (it.hasNext()) {
         sb.append(spaces(indent + 2));
         writeJson(sb, it.next(), indent + 2);
         if (it.hasNext()) sb.append(',');
         sb.append('\n');
      }
      sb.append(spaces(indent)).append(']');
   }

   private static String escapeJson(String s) {
      return s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
   }

   private static String spaces(int n) {
      return " ".repeat(n);
   }
}
