package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 16.3
 */
@Cli
public class ConfigTest {

   private static final String SAMPLE_XML =
         "<infinispan><cache-container><distributed-cache name=\"testcache\"/></cache-container></infinispan>";

   @RegisterExtension
   CliExtension cli = new CliExtension();

   @Test
   public void testSetAndGet() throws Exception {
      assertEquals(0, cli.run("config", "set", "autoconnect-url", "http://localhost:11222"));

      assertEquals(0, cli.run("config", "get", "autoconnect-url"));
      assertTrue(cli.shell().getBuffer().contains("autoconnect-url=http://localhost:11222"));
   }

   @Test
   public void testSetRemovesProperty() throws Exception {
      assertEquals(0, cli.run("config", "set", "autoconnect-url", "http://localhost:11222"));

      assertEquals(0, cli.run("config", "set", "autoconnect-url"));

      assertEquals(0, cli.run("config", "get", "autoconnect-url"));
      assertTrue(cli.shell().getBuffer().contains("autoconnect-url=null"));
   }

   @Test
   public void testReset() throws Exception {
      assertEquals(0, cli.run("config", "set", "autoconnect-url", "http://localhost:11222"));
      assertEquals(0, cli.run("config", "set", "trustall", "true"));

      assertEquals(0, cli.run("config", "reset"));

      assertEquals(0, cli.run("config", "get", "autoconnect-url"));
      assertTrue(cli.shell().getBuffer().contains("autoconnect-url=null"));
   }

   @Test
   public void testListProperties() throws Exception {
      assertEquals(0, cli.run("config", "set", "autoconnect-url", "http://localhost:11222"));
      assertEquals(0, cli.run("config", "set", "trustall", "true"));

      assertEquals(0, cli.run("config"));
      String output = cli.shell().getBuffer();
      assertTrue(output.contains("autoconnect-url=http://localhost:11222"));
      assertTrue(output.contains("trustall=true"));
   }

   @Test
   public void testGetReturnsNull() throws Exception {
      assertEquals(0, cli.run("config", "get", "nonexistent"));
      assertTrue(cli.shell().getBuffer().contains("nonexistent=null"));
   }

   @Test
   public void testConvertXmlToJson() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path inputFile = cli.configPath().resolve("test-config.xml");
      Path outputFile = cli.configPath().resolve("test-config.json");
      Files.writeString(inputFile, SAMPLE_XML);

      int rc = cli.run("config", "convert", inputFile.toString(), "-f", "json", "-o", outputFile.toString());
      assertEquals(0, rc, cli.shell().getBuffer());

      String json = Files.readString(outputFile);
      assertTrue(json.contains("distributed-cache"), json);
      assertTrue(json.contains("testcache"), json);
   }

   @Test
   public void testConvertXmlToYaml() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path inputFile = cli.configPath().resolve("test-config.xml");
      Path outputFile = cli.configPath().resolve("test-config.yaml");
      Files.writeString(inputFile, SAMPLE_XML);

      int rc = cli.run("config", "convert", inputFile.toString(), "-f", "yaml", "-o", outputFile.toString());
      assertEquals(0, rc, cli.shell().getBuffer());

      String yaml = Files.readString(outputFile);
      assertTrue(yaml.contains("distributedCache"), yaml);
      assertTrue(yaml.contains("testcache"), yaml);
   }

   @Test
   public void testConvertRoundTrip() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path xmlInput = cli.configPath().resolve("input.xml");
      Path jsonFile = cli.configPath().resolve("intermediate.json");
      Path xmlOutput = cli.configPath().resolve("output.xml");
      Files.writeString(xmlInput, SAMPLE_XML);

      int rc1 = cli.run("config", "convert", xmlInput.toString(), "-f", "json", "-o", jsonFile.toString());
      assertEquals(0, rc1, cli.shell().getBuffer());
      int rc2 = cli.run("config", "convert", jsonFile.toString(), "-f", "xml", "-o", xmlOutput.toString());
      assertEquals(0, rc2, cli.shell().getBuffer());

      String xml = Files.readString(xmlOutput);
      assertTrue(xml.contains("distributed-cache"), xml);
      assertTrue(xml.contains("testcache"), xml);
   }
}
