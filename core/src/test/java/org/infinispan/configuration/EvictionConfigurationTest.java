package org.infinispan.configuration;

import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.configuration.cache.StorageType.OFF_HEAP;
import static org.infinispan.eviction.EvictionStrategy.REMOVE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;


/**
 * Test for different scenarios of eviction configuration.
 */
@Test(groups = "unit", testName = "configuration.EvictionConfigurationTest")
public class EvictionConfigurationTest extends AbstractInfinispanTest {
   private static final ParserRegistry REGISTRY = new ParserRegistry();

   @Test
   public void testEvictionDisabled() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storage(HEAP);

      Configuration configuration = builder.build();

      assertEquals(HEAP, configuration.memory().storage());
      assertEquals(-1, configuration.memory().maxSizeBytes());
      assertEquals(-1, configuration.memory().maxCount());

      Configuration same = builder.build();
      assertEquals(configuration, same);
   }

   @Test
   public void testLegacyConfigAvailable() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxSize("1.5 GB").storage(HEAP).whenFull(REMOVE);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      Configuration configuration = builder.build();

      assertEquals(1_500_000_000, configuration.memory().maxSizeBytes());
      assertEquals(-1, configuration.memory().maxCount());
      assertEquals(REMOVE, configuration.memory().whenFull());
      assertEquals(HEAP, configuration.memory().storage());

      Configuration same = builder.build();
      assertEquals(configuration.memory(), same.memory());

      Configuration larger = builder.memory().maxSize("2.0 GB").build();

      assertEquals(2_000_000_000, larger.memory().maxSizeBytes());
      assertEquals(-1, larger.memory().maxCount());
      assertEquals(REMOVE, larger.memory().whenFull());
      assertEquals(HEAP, larger.memory().storage());
   }

   public void testUseDefaultEviction() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      Configuration configuration = builder.build();

      assertFalse(configuration.memory().isEvictionEnabled());
      assertEquals(HEAP, configuration.memory().storage());
      assertEquals(HEAP, configuration.memory().storage());
   }

   @Test
   public void testMinimal() {
      Configuration configuration = new ConfigurationBuilder().build();

      assertFalse(configuration.memory().isOffHeap());
      assertEquals(-1L, configuration.memory().maxCount());
      assertEquals(-1L, configuration.memory().maxSizeBytes());
      assertEquals(HEAP, configuration.memory().storage());
   }

   @Test
   public void testChangeFromMinimal() {
      ConfigurationBuilder initial = new ConfigurationBuilder();
      Configuration initialConfig = initial.build();
      assertEquals(HEAP, initialConfig.memory().storage());
      assertEquals(-1, initialConfig.memory().maxCount());

      initial.memory().maxCount(3);
      Configuration larger = initial.build();
      assertEquals(HEAP, larger.memory().storage());
      assertEquals(3, larger.memory().maxCount());
   }

   @Test
   public void testRuntimeConfigChanges() {
      Configuration countBounded = new ConfigurationBuilder().memory().maxCount(1000).build();
      Configuration sizeBounded = new ConfigurationBuilder().memory().maxSize("10 MB").storage(OFF_HEAP).build();

      countBounded.memory().maxCount(1200);
      sizeBounded.memory().maxSize("20MB");

      assertEquals(1200, countBounded.memory().maxCount());
      assertEquals(20_000_000, sizeBounded.memory().maxSizeBytes());
      Exceptions.expectException(CacheConfigurationException.class, () -> countBounded.memory().maxSize("30MB"));
      Exceptions.expectException(CacheConfigurationException.class, () -> sizeBounded.memory().maxCount(2000));
   }

   @Test
   public void testParseXML() {
      String xml = """
            <infinispan>
               <cache-container>
                  <local-cache name="local">
                     <memory storage="OFF_HEAP" max-size="200 MB" when-full="MANUAL" />
                  </local-cache>
               </cache-container>
            </infinispan>""";

      testSerializationAndBack(xml);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xml);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(200_000_000, afterParsing.memory().maxSizeBytes());
      assertEquals(-1, afterParsing.memory().maxCount());
      assertEquals(OFF_HEAP, afterParsing.memory().storage());
      // Remove is forced
      assertEquals(REMOVE, afterParsing.memory().whenFull());
   }

   @Test
   public void testParseXML2() {
      String xmlNew = """
            <infinispan>
               <cache-container>
                  <local-cache name="local">
                     <memory max-count="2000" when-full="REMOVE" />
                  </local-cache>
               </cache-container>
            </infinispan>""";

      testSerializationAndBack(xmlNew);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlNew);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(2000, afterParsing.memory().maxCount());
      assertEquals(REMOVE, afterParsing.memory().whenFull());
      assertEquals(HEAP, afterParsing.memory().storage());
   }

   @Test
   public void testParseXML3() {
      String xmlNew = """
            <infinispan>
               <cache-container>
                  <local-cache name="local">
                     <encoding media-type="application/json" />
                     <memory storage="HEAP" max-size="1MB" when-full="REMOVE"/>
                  </local-cache>
               </cache-container>
            </infinispan>""";

      testSerializationAndBack(xmlNew);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlNew);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertTrue(afterParsing.memory().isEvictionEnabled());
      assertEquals(HEAP, afterParsing.memory().storage());
      assertEquals(1_000_000, afterParsing.memory().maxSizeBytes());
      assertEquals(REMOVE, afterParsing.memory().whenFull());
   }


   @Test
   public void testParseJSON() {
      ConfigurationBuilderHolder holder = new ParserRegistry().parse("{\"local-cache\":{ \"memory\":{\"storage\":\"HEAP\",\"when-full\":\"REMOVE\",\"max-count\":5000}}}}", MediaType.APPLICATION_JSON);
      Configuration fromJson = holder.getCurrentConfigurationBuilder().build();
      assertEquals(-1, fromJson.memory().maxSizeBytes());
      assertEquals(5000, fromJson.memory().maxCount());
      assertEquals(HEAP, fromJson.memory().storage());
      assertEquals(REMOVE, fromJson.memory().whenFull());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".*Cannot configure both maxCount and maxSize.*")
   public void testErrorForMultipleThresholds() {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.memory().storage(OFF_HEAP).maxCount(10).maxSize("10TB").build();
   }

   private void testSerializationAndBack(String xml) {
      // Parse config
      ConfigurationBuilderHolder configurationBuilderHolder = REGISTRY.parse(xml);
      ConfigurationBuilder builder = configurationBuilderHolder.getNamedConfigurationBuilders().get("local");
      Configuration before = builder.build();

      // Serialize the parsed config
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ConfigurationWriter w = ConfigurationWriter.to(baos).build()) {
         REGISTRY.serialize(w, "local", before);
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ConfigurationBuilderHolder holderAfter = REGISTRY.parse(bais, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);

      // Parse again from the serialized
      ConfigurationBuilder afterParsing = holderAfter.getNamedConfigurationBuilders().get("local");

      Configuration after = afterParsing.build();
      assertEquals(after.memory(), before.memory());
   }
}
