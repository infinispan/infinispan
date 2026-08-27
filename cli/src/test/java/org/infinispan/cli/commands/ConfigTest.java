package org.infinispan.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
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
      CliTerminal terminal = cli.run("config", "set", "autoconnect-url", "http://localhost:11222");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "get", "autoconnect-url");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("autoconnect-url=http://localhost:11222");
   }

   @Test
   public void testSetRemovesProperty() throws Exception {
      CliTerminal terminal = cli.run("config", "set", "autoconnect-url", "http://localhost:11222");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "set", "autoconnect-url");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "get", "autoconnect-url");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("autoconnect-url=null");
   }

   @Test
   public void testReset() throws Exception {
      CliTerminal terminal = cli.run("config", "set", "autoconnect-url", "http://localhost:11222");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "set", "trustall", "true");
      assertEquals(0, terminal.exitCode());

      terminal = cli.run("config", "reset");
      assertEquals(0, terminal.exitCode());

      terminal = cli.run("config", "get", "autoconnect-url");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("autoconnect-url=null");
   }

   @Test
   public void testListProperties() throws Exception {
      CliTerminal terminal = cli.run("config", "set", "autoconnect-url", "http://localhost:11222");
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "set", "trustall", "true");
      assertEquals(0, terminal.exitCode());

      terminal = cli.run("config");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("autoconnect-url=http://localhost:11222");
      terminal.assertContains("trustall=true");
   }

   @Test
   public void testGetReturnsNull() throws Exception {
      CliTerminal terminal = cli.run("config", "get", "nonexistent");
      assertEquals(0, terminal.exitCode());
      terminal.assertContains("nonexistent=null");
   }

   @Test
   public void testConvertXmlToJson() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path inputFile = cli.configPath().resolve("test-config.xml");
      Path outputFile = cli.configPath().resolve("test-config.json");
      Files.writeString(inputFile, SAMPLE_XML);
      CliTerminal terminal = cli.run("config", "convert", inputFile.toString(), "-f", "json", "-o", outputFile.toString());
      assertEquals(0, terminal.exitCode());
      String json = Files.readString(outputFile);
      assertThat(json).contains("distributed-cache");
      assertThat(json).contains("testcache");
   }

   @Test
   public void testConvertXmlToYaml() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path inputFile = cli.configPath().resolve("test-config.xml");
      Path outputFile = cli.configPath().resolve("test-config.yaml");
      Files.writeString(inputFile, SAMPLE_XML);

      CliTerminal terminal = cli.run("config", "convert", inputFile.toString(), "-f", "yaml", "-o", outputFile.toString());
      assertEquals(0, terminal.exitCode());

      String yaml = Files.readString(outputFile);
      assertThat(yaml).contains("distributedCache");
      assertThat(yaml).contains("testcache");
   }

   @Test
   public void testConvertRoundTrip() throws IOException {
      Assumptions.assumeFalse(cli.isProcess(), "config convert requires full core infrastructure unavailable in native mode");
      Path xmlInput = cli.configPath().resolve("input.xml");
      Path jsonFile = cli.configPath().resolve("intermediate.json");
      Path xmlOutput = cli.configPath().resolve("output.xml");
      Files.writeString(xmlInput, SAMPLE_XML);

      CliTerminal terminal = cli.run("config", "convert", xmlInput.toString(), "-f", "json", "-o", jsonFile.toString());
      assertEquals(0, terminal.exitCode());
      terminal = cli.run("config", "convert", jsonFile.toString(), "-f", "xml", "-o", xmlOutput.toString());
      assertEquals(0, terminal.exitCode());
      String xml = Files.readString(xmlOutput);
      assertThat(xml).contains("distributed-cache");
      assertThat(xml).contains("testcache");
   }
}
