package org.infinispan.configuration.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.maven.MavenArtifact;
import org.infinispan.commons.maven.MavenSettings;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.serializer.SchemaVersionAuditTest")
public class SchemaVersionAuditTest extends AbstractInfinispanTest {

   private static final String BASELINE_SCHEMA_ARTIFACT_VERSION = "16.2.1";
   private static final String BASELINE_SCHEMA_VERSION = "16.2";
   private static final Set<String> EXPLICIT_REMOVAL = Set.of(".persistence.connection-interval");
   private Json schema = null;

   @Test(dataProvider = "configurationFiles")
   public void testConfigurationHasNoUndeclaredSinceAttributes(Path config) throws Exception {
      Json schema = fetchBaselineSchema();
      Set<String> knownNames = SchemaVersionAudit.knownNames(schema);

      URL configUrl = FileLookupFactory.newInstance().lookupFileLocation(config.toString(), Thread.currentThread().getContextClassLoader());
      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", System.getProperty("java.io.tmpdir"));
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      ConfigurationBuilderHolder holder = registry.parse(configUrl);

      List<Configuration> configurations = new ArrayList<>();
      Set<String> actuallySerializedNames = new HashSet<>();
      for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
         Configuration configuration = entry.getValue().build();
         configurations.add(configuration);
         actuallySerializedNames.addAll(SchemaVersionAudit.namesInXml(configuration.toStringConfiguration(entry.getKey())));
      }

      Set<String> violations = new TreeSet<>();
      for (Configuration configuration : configurations) {
         violations.addAll(SchemaVersionAudit.undeclaredSinceViolations(knownNames, actuallySerializedNames, configuration, EXPLICIT_REMOVAL));
      }

      assertThat(violations)
            .as("attributes/elements missing from the published 16.2 schema must carry an explicit .since() newer than the untagged default")
            .isEmpty();
   }

   public void testKnownNamesExtractsLeafPropertyNamesOnly() {
      Json schema = Json.read("""
            {
              "$defs": {
                "StateTransfer": {
                  "type": "object",
                  "properties": {
                    "chunk-size": { "type": "string" },
                    "await-initial-transfer": { "type": "boolean" }
                  }
                }
              }
            }
            """);

      Set<String> names = SchemaVersionAudit.knownNames(schema);

      assertThat(names).contains("chunk-size", "await-initial-transfer");
      assertThat(names).doesNotContain("StateTransfer", "properties", "type", "$defs");
   }

   private Json fetchBaselineSchema() {
      if (schema != null)
         return schema;

      return schema = fetchBaselineSchemaInternal();
   }

   private static Json fetchBaselineSchemaInternal() {
      MavenSettings.init();

      Path zip;
      try {
         zip = new MavenArtifact("org.infinispan", "infinispan-distribution", BASELINE_SCHEMA_ARTIFACT_VERSION, "json")
               .resolveArtifact("zip");
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }

      if (zip == null)
         throw new SkipException("Could not resolve org.infinispan:infinispan-distribution:"
               + BASELINE_SCHEMA_ARTIFACT_VERSION + ":json (no network access or artifact missing).");

      try {
         URL url = new URL("jar:" + zip.toUri() + "!/infinispan-config-" + BASELINE_SCHEMA_VERSION + ".json");
         return Json.read(url);
      } catch (MalformedURLException e) {
         throw new IllegalStateException(e);
      }
   }

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      URL configDir = Thread.currentThread().getContextClassLoader().getResource("configs/all");
      List<Path> paths = Files.list(Paths.get(configDir.toURI())).toList();
      Object[][] configurationFiles = new Object[paths.size()][];
      boolean hasCurrentSchema = false;
      for (int i = 0; i < paths.size(); i++) {
         if (paths.get(i).getFileName().toString().equals(Version.getSchemaVersion() + ".xml")) {
            hasCurrentSchema = true;
         }
         configurationFiles[i] = new Object[]{paths.get(i)};
      }
      // Ensure that we contain the current schema version at the very least
      assertTrue(hasCurrentSchema, "Could not find a '" + Version.getSchemaVersion() + ".xml' configuration file");

      return configurationFiles;
   }
}
