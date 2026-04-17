package org.infinispan.schematools;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.internal.Json;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Validates that JSON schema definitions stay in sync with their XSD equivalents.
 * <p>
 * Discovers all XSD+JSON schema pairs under the project and compares:
 * <ul>
 *   <li>Type definitions: each XSD complexType should have a corresponding JSON $defs entry</li>
 *   <li>Properties: each XSD attribute/child-element should have a corresponding JSON property</li>
 * </ul>
 * <p>
 * All JSON $defs names use PascalCase derived from XSD kebab-case complexType names via
 * {@link NamingStrategy#PASCAL_CASE}. Known structural differences between the two formats
 * are handled via exclusion sets. When a new type or property is added to one schema but not
 * the other, this test will fail.
 *
 * @since 16.2
 */
@Tag("functional")
public class SchemaComparisonTest {

   private static final String XS_NS = "http://www.w3.org/2001/XMLSchema";
   private static final Path PROJECT_ROOT = findProjectRoot();

   // Schema directories relative to project root
   private static final String[] SCHEMA_MODULES = {
         "core/src/main/resources/schema",
         "anchored-keys/src/main/resources/schema",
         "counter/src/main/resources/schema",
         "lock/src/main/resources/schema",
         "persistence/jdbc-common/src/main/resources/schema",
         "persistence/jdbc/src/main/resources/schema",
         "persistence/remote/src/main/resources/schema",
         "persistence/rocksdb/src/main/resources/schema",
         "persistence/sql/src/main/resources/schema",
         "server/runtime/src/main/resources/schema",
   };

   // --- Exclusion sets keyed by schema base name (e.g. "infinispan-config-16.2") ---

   // XSD complexType names intentionally not in JSON $defs
   private static final Map<String, Set<String>> XSD_TYPES_EXCLUDED = Map.ofEntries(
         Map.entry("infinispan-config-16.2", Set.of(
               // Abstract base types (decomposed into allOf/$ref in JSON)
               "cache", "clustered-cache", "encodingType", "store", "loader",
               // JGroups stack types (separate jgroups-config JSON schema)
               "jgroups", "jgroups-stack", "jgroups-remote-sites", "jgroups-stack-relay-remote-site",
               // Global tracing (JSON uses per-cache CacheTracing)
               "tracing",
               // Deprecated/legacy types
               "custom-interceptors", "store-as-binary", "name-rewriter",
               "principal-transformer", "regex-principal-transformer", "cluster-role-mapper",
               // XSD-only structural helpers
               "property", "remote-server", "roles"
         )),
         Map.entry("infinispan-counters-config-16.2", Set.of(
               // Abstract base type
               "counter"
         )),
         Map.entry("infinispan-server-16.2", Set.of(
               // XSD-only structural helpers
               "property", "boolean-element", "name-listType", "qop-listType",
               // Named protocol connector (abstract base in XSD)
               "named-protocol-connector", "protocol-connector",
               // Abstract base types
               "ldap-attribute", "principal-transformer",
               // Inline structural types
               "prefix", "security-realm-authentication", "security-realm-authorization"
         )),
         Map.entry("infinispan-cachestore-remote-config-16.2", Set.of(
               // Abstract base type
               "basekeystore",
               // JSON uses RemoteConnectionPool (naming mismatch)
               "connectionPool",
               // Deprecated
               "executorFactory"
         ))
   );

   // JSON $defs names intentionally not in XSD
   private static final Map<String, Set<String>> JSON_TYPES_EXCLUDED = Map.ofEntries(
         Map.entry("infinispan-config-16.2", Set.of(
               // Structural wrappers for named-map pattern
               "CacheDefinition", "BackupSite", "ThreadFactoryWrapper",
               "ThreadPoolDefinition", "RoleDefinition",
               // Decompositions of abstract XSD types
               "CommonCacheAttributes", "ClusteredCacheAttributes", "AbstractStore",
               // Promoted from anonymous inline or abstract XSD definitions
               "CacheAuthorization", "GlobalAuthorization", "Indexing",
               "PartitionHandling", "ThreadFactory", "XSiteStateTransfer", "EncodingType",
               // JSON-only types
               "AllowList", "BoundedQueueThreadPool", "SimpleThreadPool",
               "EvictionContainers", "EvictionContainerDefinition",
               "BackupFor", "CacheTracing", "Query"
         )),
         Map.entry("infinispan-server-16.2", Set.of(
               // Unreferenced base type
               "ProtocolConnectorProperties",
               // Structural helper
               "Property"
         )),
         Map.entry("infinispan-cachestore-jdbc-common-config-16.2", Set.of(
               "WriteBehind" // re-exported from core
         )),
         Map.entry("infinispan-cachestore-remote-config-16.2", Set.of(
               // JSON-only types for remote store
               "RemoteConnectionPool", "RemoteServer", "WriteBehind"
         )),
         Map.entry("infinispan-cachestore-rocksdb-config-16.2", Set.of(
               "WriteBehind" // re-exported from core
         )),
         Map.entry("infinispan-cachestore-sql-config-16.2", Set.of(
               "AbstractSqlStore", // abstract base decomposed in JSON
               "QueryJdbcStore", "TableJdbcStore" // anonymous inline types in XSD
         )),
         Map.entry("infinispan-counters-config-16.2", Set.of(
               "AbstractCounter" // abstract base decomposed in JSON
         ))
   );

   @SafeVarargs
   private static Set<String> union(Set<String>... sets) {
      Set<String> result = new TreeSet<>();
      for (Set<String> s : sets) result.addAll(s);
      return result;
   }

   // Properties inherited from abstract 'cache' XSD type (JSON uses allOf/$ref instead)
   private static final Set<String> CACHE_BASE_PROPS = Set.of(
         "aliases", "backup-for", "backups", "configuration", "custom-interceptors",
         "encoding", "expiration", "indexing", "locking", "memory", "name",
         "persistence", "query", "security", "statistics", "store-as-binary",
         "tracing", "transaction", "unreliable-return-values"
   );

   // Additional properties inherited from 'clustered-cache' XSD type
   private static final Set<String> CLUSTERED_CACHE_EXTRA_PROPS = Set.of(
         "mode", "partition-handling", "remote-timeout"
   );

   // Common store properties inherited cross-schema from core config:store
   private static final Set<String> STORE_INHERITED_PROPS = Set.of(
         "shared", "preload", "purge", "read-only", "write-only",
         "transactional", "max-batch-size", "segmented", "write-behind", "properties"
   );

   // XSD properties to skip per schema + JSON type name
   private static final Map<String, Map<String, Set<String>>> XSD_PROPS_EXCLUDED = Map.ofEntries(
         Map.entry("infinispan-config-16.2", Map.ofEntries(
               Map.entry("CacheContainer", Set.of(
                     "name", "jndi-name", "module", "start", "count", "size", "tracing",
                     "local-cache", "local-cache-configuration",
                     "invalidation-cache", "invalidation-cache-configuration",
                     "replicated-cache", "replicated-cache-configuration",
                     "distributed-cache", "distributed-cache-configuration"
               )),
               Map.entry("Transport", Set.of("property")),
               Map.entry("GlobalSecurity", Set.of(
                     "audit-logger", "group-only-mapping", "roles", "class",
                     "cluster-permission-mapper", "custom-permission-mapper",
                     "identity-role-mapper", "common-name-role-mapper", "cluster-role-mapper",
                     "custom-role-mapper", "name-rewriter"
               )),
               Map.entry("CacheSecurity", Set.of("enabled", "roles")),
               Map.entry("GlobalState", Set.of("class")),
               Map.entry("Memory", Set.of("size", "type")),
               Map.entry("Backups", Set.of("backup")),
               Map.entry("Backup", Set.of("site", "chunk-size", "max-retries", "mode", "wait-time")),
               Map.entry("Groups", Set.of("class")),
               Map.entry("Jmx", Set.of("property")),
               Map.entry("Serialization", Set.of("class", "regex", "context-initializer")),
               Map.entry("Threads", Set.of(
                     "thread-factory", "non-blocking-bounded-queue-thread-pool",
                     "blocking-bounded-queue-thread-pool", "scheduled-thread-pool", "cached-thread-pool",
                     "name", "group-name", "thread-name-pattern", "priority",
                     "max-threads", "core-threads", "keepalive-time", "queue-length"
               )),
               Map.entry("LocalCache", CACHE_BASE_PROPS),
               Map.entry("DistributedCache", union(CACHE_BASE_PROPS, CLUSTERED_CACHE_EXTRA_PROPS)),
               Map.entry("ReplicatedCache", union(CACHE_BASE_PROPS, CLUSTERED_CACHE_EXTRA_PROPS)),
               Map.entry("InvalidationCache", union(CACHE_BASE_PROPS, CLUSTERED_CACHE_EXTRA_PROPS)),
               Map.entry("FileStore", Set.of("max-entries", "property")),
               Map.entry("CustomStore", Set.of("property")),
               Map.entry("IndexType", Set.of("segments"))
         )),
         Map.entry("infinispan-cachestore-remote-config-16.2", Map.of(
               "RemoteCacheContainer", Set.of("property"),
               "RemoteStore", Set.of("async-executor")
         )),
         Map.entry("infinispan-server-16.2", Map.of(
               // Server schema: inline children and structural differences
               "Security", Set.of("security-realm", "credential-store"),
               "SecurityRealm", Set.of("realm-name", "keytab")
         ))
   );

   // JSON properties to skip per schema + JSON type name
   private static final Map<String, Map<String, Set<String>>> JSON_PROPS_EXCLUDED = Map.ofEntries(
         Map.entry("infinispan-config-16.2", Map.ofEntries(
               Map.entry("CacheContainer", Set.of("caches")),
               Map.entry("Threads", Set.of("thread-factories", "thread-pools")),
               Map.entry("GlobalState", Set.of(
                     "immutable-configuration-storage", "volatile-configuration-storage",
                     "overlay-configuration-storage", "managed-configuration-storage",
                     "custom-configuration-storage"
               )),
               Map.entry("Persistence", Set.of("file-store", "store")),
               Map.entry("Jmx", Set.of("properties")),
               Map.entry("Serialization", Set.of("context-initializers")),
               Map.entry("FileStore", Set.of(
                     "shared", "preload", "purge", "read-only", "write-only",
                     "transactional", "max-batch-size", "segmented", "write-behind", "properties"
               )),
               Map.entry("CustomStore", Set.of(
                     "shared", "preload", "purge", "read-only", "write-only",
                     "transactional", "max-batch-size", "segmented", "write-behind", "properties"
               )),
               Map.entry("DistributedCache", Set.of("partition-handling"))
         )),
         Map.entry("infinispan-cachestore-jdbc-common-config-16.2", Map.of(
               // Inherited from core config:store
               "AbstractJdbcStore", STORE_INHERITED_PROPS
         )),
         Map.entry("infinispan-cachestore-jdbc-config-16.2", Map.ofEntries(
               // Inherited from jdbc-common and core config:store
               Map.entry("StringKeyedJdbcStore", Set.of(
                     "shared", "preload", "purge", "read-only", "write-only",
                     "transactional", "max-batch-size", "segmented", "write-behind", "properties",
                     "cdi-data-source", "connection-pool", "data-source", "db-major-version",
                     "db-minor-version", "dialect", "read-query-timeout",
                     "simple-connection", "write-query-timeout"
               ))
         )),
         Map.entry("infinispan-cachestore-remote-config-16.2", Map.ofEntries(
               // Inherited from core config:store
               Map.entry("RemoteStore", STORE_INHERITED_PROPS),
               // Inherited from basekeystore
               Map.entry("Keystore", Set.of("filename", "password", "type")),
               Map.entry("Truststore", Set.of("filename", "password", "type")),
               // Inherited from core
               Map.entry("RemoteCacheContainer", Set.of("properties")),
               // Child elements as properties
               Map.entry("Authentication", Set.of("digest", "external", "plain"))
         )),
         Map.entry("infinispan-cachestore-rocksdb-config-16.2", Map.of(
               // Inherited from core config:store
               "RocksdbStore", STORE_INHERITED_PROPS
         ))
   );

   @TestFactory
   Stream<DynamicTest> schemaComparison() {
      List<DynamicTest> tests = new ArrayList<>();
      for (String moduleDir : SCHEMA_MODULES) {
         Path dir = PROJECT_ROOT.resolve(moduleDir);
         if (!Files.isDirectory(dir)) continue;
         discoverSchemaPairs(dir).forEach((baseName, pair) -> {
            tests.add(DynamicTest.dynamicTest(
                  baseName + ": XSD types have JSON equivalent",
                  () -> assertXsdTypesHaveJsonEquivalent(baseName, pair)));
            tests.add(DynamicTest.dynamicTest(
                  baseName + ": JSON types have XSD equivalent",
                  () -> assertJsonTypesHaveXsdEquivalent(baseName, pair)));
            tests.add(DynamicTest.dynamicTest(
                  baseName + ": XSD properties exist in JSON",
                  () -> assertXsdPropertiesExistInJson(baseName, pair)));
            tests.add(DynamicTest.dynamicTest(
                  baseName + ": JSON properties exist in XSD",
                  () -> assertJsonPropertiesExistInXsd(baseName, pair)));
         });
      }
      assertThat(tests).as("Should discover at least one schema pair").isNotEmpty();
      return tests.stream();
   }

   // --- Assertions ---

   private void assertXsdTypesHaveJsonEquivalent(String schema, SchemaPair pair) throws Exception {
      Set<String> xsdExcl = XSD_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Set<String> missingInJson = new TreeSet<>();
      for (String xsdType : pair.xsdTypes().keySet()) {
         if (xsdExcl.contains(xsdType)) continue;
         String jsonName = resolveJsonName(xsdType);
         if (!pair.jsonTypes().containsKey(jsonName)) {
            missingInJson.add(xsdType + " (expected JSON: " + jsonName + ")");
         }
      }
      assertThat(missingInJson)
            .as("XSD complexTypes missing from JSON $defs in %s", schema)
            .isEmpty();
   }

   private void assertJsonTypesHaveXsdEquivalent(String schema, SchemaPair pair) throws Exception {
      Set<String> xsdExcl = XSD_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Set<String> jsonExcl = JSON_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Set<String> xsdJsonNames = new TreeSet<>();
      for (String xsdType : pair.xsdTypes().keySet()) {
         if (!xsdExcl.contains(xsdType)) xsdJsonNames.add(resolveJsonName(xsdType));
      }
      Set<String> missingInXsd = new TreeSet<>();
      for (String jsonType : pair.jsonTypes().keySet()) {
         if (jsonExcl.contains(jsonType)) continue;
         if (!xsdJsonNames.contains(jsonType)) missingInXsd.add(jsonType);
      }
      assertThat(missingInXsd)
            .as("JSON $defs types missing from XSD complexTypes in %s", schema)
            .isEmpty();
   }

   private void assertXsdPropertiesExistInJson(String schema, SchemaPair pair) throws Exception {
      Set<String> xsdExcl = XSD_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Map<String, Set<String>> propsExcl = XSD_PROPS_EXCLUDED.getOrDefault(schema, Map.of());
      Set<String> mismatches = new TreeSet<>();
      for (Map.Entry<String, Set<String>> entry : pair.xsdTypes().entrySet()) {
         if (xsdExcl.contains(entry.getKey())) continue;
         String jsonName = resolveJsonName(entry.getKey());
         Set<String> jsonProps = pair.jsonTypes().get(jsonName);
         if (jsonProps == null) continue;
         Set<String> excluded = propsExcl.getOrDefault(jsonName, Set.of());
         for (String xsdProp : entry.getValue()) {
            if (!excluded.contains(xsdProp) && !jsonProps.contains(xsdProp))
               mismatches.add(jsonName + "." + xsdProp);
         }
      }
      assertThat(mismatches)
            .as("XSD attributes/elements missing from JSON properties in %s", schema)
            .isEmpty();
   }

   private void assertJsonPropertiesExistInXsd(String schema, SchemaPair pair) throws Exception {
      Set<String> xsdExcl = XSD_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Set<String> jsonExcl = JSON_TYPES_EXCLUDED.getOrDefault(schema, Set.of());
      Map<String, Set<String>> propsExcl = JSON_PROPS_EXCLUDED.getOrDefault(schema, Map.of());
      Map<String, Set<String>> xsdPropsByJsonName = new TreeMap<>();
      for (Map.Entry<String, Set<String>> entry : pair.xsdTypes().entrySet()) {
         if (xsdExcl.contains(entry.getKey())) continue;
         String jsonName = resolveJsonName(entry.getKey());
         xsdPropsByJsonName.merge(jsonName, new TreeSet<>(entry.getValue()), (a, b) -> {
            a.addAll(b);
            return a;
         });
      }
      Set<String> mismatches = new TreeSet<>();
      for (Map.Entry<String, Set<String>> entry : pair.jsonTypes().entrySet()) {
         if (jsonExcl.contains(entry.getKey())) continue;
         Set<String> xsdProps = xsdPropsByJsonName.get(entry.getKey());
         if (xsdProps == null) continue;
         Set<String> excluded = propsExcl.getOrDefault(entry.getKey(), Set.of());
         for (String jsonProp : entry.getValue()) {
            if (!excluded.contains(jsonProp) && !xsdProps.contains(jsonProp))
               mismatches.add(entry.getKey() + "." + jsonProp);
         }
      }
      assertThat(mismatches)
            .as("JSON properties missing from XSD attributes/elements in %s", schema)
            .isEmpty();
   }

   // --- Schema pair discovery ---

   record SchemaPair(Map<String, Set<String>> xsdTypes, Map<String, String> xsdExtensionBases,
                      Map<String, Set<String>> jsonTypes) {}

   private static Map<String, SchemaPair> discoverSchemaPairs(Path dir) {
      Map<String, SchemaPair> pairs = new LinkedHashMap<>();
      try (var files = Files.list(dir)) {
         files.filter(p -> p.toString().endsWith(".xsd"))
               .forEach(xsdFile -> {
                  String baseName = xsdFile.getFileName().toString().replace(".xsd", "");
                  Path jsonFile = dir.resolve(baseName + ".json");
                  if (Files.exists(jsonFile)) {
                     try {
                        XsdParseResult xsd = parseXsd(xsdFile);
                        resolveInheritance(xsd.types(), xsd.extensionBases());
                        pairs.put(baseName, new SchemaPair(xsd.types(), xsd.extensionBases(),
                              parseJsonSchema(jsonFile)));
                     } catch (Exception e) {
                        throw new RuntimeException("Failed to parse schemas for " + baseName, e);
                     }
                  }
               });
      } catch (IOException e) {
         throw new RuntimeException("Failed to list schema directory: " + dir, e);
      }
      return pairs;
   }

   // --- Name resolution ---

   private static String resolveJsonName(String xsdTypeName) {
      return NamingStrategy.PASCAL_CASE.convert(xsdTypeName);
   }

   // --- XSD Parsing ---

   private static XsdParseResult parseXsd(Path xsdFile) throws Exception {
      Map<String, Set<String>> types = new LinkedHashMap<>();
      Map<String, String> extensionBases = new LinkedHashMap<>();
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      Document doc = dbf.newDocumentBuilder().parse(new FileInputStream(xsdFile.toFile()));

      NodeList complexTypes = doc.getElementsByTagNameNS(XS_NS, "complexType");
      for (int i = 0; i < complexTypes.getLength(); i++) {
         Element ct = (Element) complexTypes.item(i);
         String name = ct.getAttribute("name");
         if (name.isEmpty()) continue;
         Set<String> properties = new LinkedHashSet<>();
         String base = collectXsdProperties(ct, properties);
         types.put(name, properties);
         if (base != null) {
            int colonIdx = base.indexOf(':');
            extensionBases.put(name, colonIdx >= 0 ? base.substring(colonIdx + 1) : base);
         }
      }
      return new XsdParseResult(types, extensionBases);
   }

   record XsdParseResult(Map<String, Set<String>> types, Map<String, String> extensionBases) {}

   /**
    * Resolves same-schema XSD type inheritance by merging base type properties into derived types.
    */
   private static void resolveInheritance(Map<String, Set<String>> types,
         Map<String, String> extensionBases) {
      for (String typeName : types.keySet()) {
         String current = typeName;
         while (extensionBases.containsKey(current)) {
            String base = extensionBases.get(current);
            Set<String> baseProps = types.get(base);
            if (baseProps != null) {
               types.get(typeName).addAll(baseProps);
               current = base;
            } else {
               break;
            }
         }
      }
   }

   private static String collectXsdProperties(Element complexType, Set<String> properties) {
      NodeList attrs = complexType.getElementsByTagNameNS(XS_NS, "attribute");
      for (int i = 0; i < attrs.getLength(); i++) {
         Element attr = (Element) attrs.item(i);
         if (isNestedInAnonymousType(attr, complexType)) continue;
         String name = attr.getAttribute("name");
         if (!name.isEmpty() && !name.startsWith("stack.")) properties.add(name);
      }
      return collectDirectChildElements(complexType, properties);
   }

   private static boolean isNestedInAnonymousType(Element element, Element owningComplexType) {
      Node parent = element.getParentNode();
      while (parent != null && parent != owningComplexType) {
         if (parent instanceof Element el
               && "complexType".equals(el.getLocalName())
               && XS_NS.equals(el.getNamespaceURI())
               && el.getAttribute("name").isEmpty()) {
            return true;
         }
         parent = parent.getParentNode();
      }
      return false;
   }

   private static String collectDirectChildElements(Element complexType, Set<String> properties) {
      String extensionBase = null;
      for (Node child = complexType.getFirstChild(); child != null; child = child.getNextSibling()) {
         if (child instanceof Element el && XS_NS.equals(el.getNamespaceURI())) {
            switch (el.getLocalName()) {
               case "sequence", "all", "choice" -> collectElementsFromGroup(el, properties);
               case "complexContent" -> {
                  for (Node cc = el.getFirstChild(); cc != null; cc = cc.getNextSibling()) {
                     if (cc instanceof Element ext && XS_NS.equals(ext.getNamespaceURI())
                           && "extension".equals(ext.getLocalName())) {
                        extensionBase = ext.getAttribute("base");
                        for (Node ec = ext.getFirstChild(); ec != null; ec = ec.getNextSibling()) {
                           if (ec instanceof Element group && XS_NS.equals(group.getNamespaceURI())) {
                              switch (group.getLocalName()) {
                                 case "sequence", "all", "choice" ->
                                       collectElementsFromGroup(group, properties);
                              }
                           }
                        }
                     }
                  }
               }
            }
         }
      }
      return extensionBase;
   }

   private static void collectElementsFromGroup(Element group, Set<String> properties) {
      for (Node child = group.getFirstChild(); child != null; child = child.getNextSibling()) {
         if (child instanceof Element el && XS_NS.equals(el.getNamespaceURI())) {
            switch (el.getLocalName()) {
               case "element" -> {
                  String name = el.getAttribute("name");
                  if (!name.isEmpty()) properties.add(name);
               }
               case "sequence", "all", "choice" -> collectElementsFromGroup(el, properties);
            }
         }
      }
   }

   // --- JSON Schema Parsing ---

   private static Map<String, Set<String>> parseJsonSchema(Path jsonFile) throws IOException {
      Map<String, Set<String>> types = new LinkedHashMap<>();
      Json schema = Json.read(Files.readString(jsonFile));
      Json defs = schema.at("$defs");
      if (defs == null || !defs.isObject()) return types;
      for (Map.Entry<String, Json> entry : defs.asJsonMap().entrySet()) {
         types.put(entry.getKey(), collectJsonProperties(entry.getValue()));
      }
      return types;
   }

   private static Set<String> collectJsonProperties(Json node) {
      Set<String> props = new LinkedHashSet<>();
      if (node == null || !node.isObject()) return props;
      Json properties = node.at("properties");
      if (properties != null && properties.isObject()) {
         props.addAll(properties.asJsonMap().keySet());
      }
      Json allOf = node.at("allOf");
      if (allOf != null && allOf.isArray()) {
         for (Json element : allOf.asJsonList()) {
            if (element.isObject() && element.has("properties")) {
               props.addAll(element.at("properties").asJsonMap().keySet());
            }
         }
      }
      return props;
   }

   private static Path findProjectRoot() {
      Path dir = Path.of("").toAbsolutePath();
      while (dir != null) {
         if (Files.isDirectory(dir.resolve("core/src/main/resources/schema"))) return dir;
         dir = dir.getParent();
      }
      throw new IllegalStateException("Cannot find project root");
   }
}
