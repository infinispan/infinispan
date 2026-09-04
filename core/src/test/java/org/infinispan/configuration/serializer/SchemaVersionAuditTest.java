package org.infinispan.configuration.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
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

   private static final String BASELINE_SCHEMA_URL = "https://infinispan.org/schemas/infinispan-config-16.2.json";
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
      URL url;
      try {
         url = new URL(BASELINE_SCHEMA_URL);
      } catch (MalformedURLException e) {
         throw new IllegalStateException(e);
      }
      try {
         return Json.read(url);
      } catch (RuntimeException e) {
         Throwable cause = e.getCause();
         if (cause instanceof UnknownHostException || cause instanceof ConnectException
               || cause instanceof SocketTimeoutException || cause instanceof NoRouteToHostException) {
            throw new SkipException("No network access to fetch " + BASELINE_SCHEMA_URL, cause);
         }
         throw e;
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
