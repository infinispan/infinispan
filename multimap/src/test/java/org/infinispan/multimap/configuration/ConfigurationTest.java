package org.infinispan.multimap.configuration;

import static org.infinispan.multimap.configuration.ConfigurationParserTest.assertMultimapConfiguration;
import static org.junit.Assert.assertEquals;

import java.io.StringWriter;
import java.util.Map;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "multimap.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testValidConfiguration() {
      MultimapCacheManagerConfigurationBuilder builder = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null);
      builder
            .addMultimap()
            .name("m1")
            .supportsDuplicates(true);
      builder.addMultimap()
            .name("m2")
            .supportsDuplicates(false);
      builder.addMultimap()
            .name("m3");

      builder.validate();
      MultimapCacheManagerConfiguration configuration = builder.create();
      assertEquals(3, configuration.multimaps().size());

      Map<String, EmbeddedMultimapConfiguration> configurations = configuration.multimaps();
      assertMultimapConfiguration(configurations.get("m1"), "m1", true);
      assertMultimapConfiguration(configurations.get("m2"), "m2", false);
      assertMultimapConfiguration(configurations.get("m3"), "m3", false);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "ISPN031002: Duplicated name 'm1' for multimap")
   public void testDuplicateName() {
      MultimapCacheManagerConfigurationBuilder builder = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null);
      builder
            .addMultimap()
            .name("m1")
            .supportsDuplicates(true);
      builder.addMultimap()
            .name("m1")
            .supportsDuplicates(false);

      builder.validate();
   }

   public void testEmptyName() {
      MultimapCacheManagerConfigurationBuilder emptyStringName = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null);
      emptyStringName
            .addMultimap()
            .name("");
      Exceptions.expectException(IllegalArgumentException.class, "ISPN031001: The multimap name is missing.", emptyStringName::validate);


      MultimapCacheManagerConfigurationBuilder nullName = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null);
      nullName
            .addMultimap();
      Exceptions.expectException(IllegalArgumentException.class, "ISPN031001: The multimap name is missing.", nullName::validate);
   }

   public void testConfigurationSerialization() {
      MultimapCacheManagerConfigurationBuilder builder = new MultimapCacheManagerConfigurationBuilder((GlobalConfigurationBuilder) null);
      builder
            .addMultimap()
            .name("m1")
            .supportsDuplicates(true);
      builder.addMultimap()
            .name("m2")
            .supportsDuplicates(false);
      builder.addMultimap()
            .name("m3");

      builder.validate();
      MultimapCacheManagerConfiguration configuration = builder.create();
      assertEquals(3, configuration.multimaps().size());
      ParserRegistry registry = new ParserRegistry();

      assertYamlSerialization(configuration, registry);
      assertJsonSerialization(configuration, registry);
      assertXmlSerialization(configuration, registry);
   }

   private void assertYamlSerialization(MultimapCacheManagerConfiguration configuration, ParserRegistry registry) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(MediaType.APPLICATION_YAML).build()) {
         registry.serializeWith(writer, new MultimapCacheManagerConfigurationSerializer(), configuration);
      }
      assertEquals("multimaps: \n" +
            "  m1: \n" +
            "    multimap: \n" +
            "      supportsDuplicates: \"true\"\n" +
            "  m2: \n" +
            "    multimap: \n" +
            "      supportsDuplicates: \"false\"\n" +
            "  m3: \n" +
            "    multimap: ~\n", sw.toString());
   }

   private void assertJsonSerialization(MultimapCacheManagerConfiguration configuration, ParserRegistry registry) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(MediaType.APPLICATION_JSON).build()) {
         registry.serializeWith(writer, new MultimapCacheManagerConfigurationSerializer(), configuration);
      }
      assertEquals("{\"multimaps\":{\"m1\":{\"multimap\":{\"supports-duplicates\":true}},\"m2\":{\"multimap\":{\"supports-duplicates\":false}},\"m3\":{\"multimap\":{}}}}", sw.toString());
   }


   private void assertXmlSerialization(MultimapCacheManagerConfiguration configuration, ParserRegistry registry) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(MediaType.APPLICATION_XML).build()) {
         registry.serializeWith(writer, new MultimapCacheManagerConfigurationSerializer(), configuration);
      }
      assertEquals("<?xml version=\"1.0\"?><multimaps xmlns=\"urn:infinispan:config:multimaps:15.0\"><multimap name=\"m1\" supports-duplicates=\"true\"/><multimap name=\"m2\" supports-duplicates=\"false\"/><multimap name=\"m3\"/></multimaps>", sw.toString());
   }
}
