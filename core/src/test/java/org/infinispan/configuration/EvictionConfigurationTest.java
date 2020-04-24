package org.infinispan.configuration;

import static org.infinispan.configuration.cache.StorageType.BINARY;
import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.configuration.cache.StorageType.OBJECT;
import static org.infinispan.configuration.cache.StorageType.OFF_HEAP;
import static org.infinispan.eviction.EvictionStrategy.REMOVE;
import static org.infinispan.eviction.EvictionType.COUNT;
import static org.infinispan.eviction.EvictionType.MEMORY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryStorageConfiguration;
import org.infinispan.configuration.cache.StorageType;
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
   private static final JsonReader JSON_READER = new JsonReader();

   @Test
   public void testReuseLegacyBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().size(200);

      Configuration configuration = builder.build();

      assertEquals(configuration.memory().maxSizeBytes(), -1);
      assertEquals(configuration.memory().maxCount(), 200);
      assertEquals(configuration.memory().storageType(), OBJECT);

      Configuration fromSameBuilder = builder.build();
      assertEquals(configuration, fromSameBuilder);
   }

   @Test
   public void testReuseChangeLegacy() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storageType(OBJECT).size(12);

      Configuration conf1 = builder.build();
      assertEquals(conf1.memory().storageType(), OBJECT);
      assertEquals(conf1.memory().size(), 12);

      builder.memory().storageType(BINARY);
      Configuration build2 = builder.build();
      assertEquals(build2.memory().storageType(), BINARY);
      assertEquals(build2.memory().size(), 12);
   }

   @Test
   public void testEvictionDisabled() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storageType(StorageType.BINARY);

      Configuration configuration = builder.build();

      assertEquals(configuration.memory().storageType(), StorageType.BINARY);
      assertEquals(configuration.memory().maxSizeBytes(), -1);
      assertEquals(configuration.memory().maxCount(), -1);

      Configuration same = builder.build();
      assertEquals(configuration, same);
   }

   @Test
   public void testLegacyConfigAvailable() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxSize("1.5 GB").storage(HEAP).whenFull(REMOVE);

      Configuration configuration = builder.build();

      assertEquals(configuration.memory().maxSizeBytes(), 1_500_000_000);
      assertEquals(configuration.memory().maxCount(), -1);
      assertEquals(configuration.memory().whenFull(), REMOVE);
      assertEquals(configuration.memory().storageType(), OBJECT);
      assertEquals(configuration.memory().evictionStrategy(), REMOVE);
      assertEquals(configuration.memory().size(), 1_500_000_000);
      assertEquals(configuration.memory().evictionType(), MEMORY);

      Configuration same = builder.build();
      assertEquals(configuration.memory(), same.memory());

      Configuration larger = builder.memory().maxSize("2.0 GB").build();

      assertEquals(larger.memory().maxSizeBytes(), 2_000_000_000);
      assertEquals(larger.memory().maxCount(), -1);
      assertEquals(larger.memory().whenFull(), REMOVE);
      assertEquals(larger.memory().storage(), HEAP);
      assertEquals(larger.memory().storageType(), OBJECT);
      assertEquals(larger.memory().evictionStrategy(), REMOVE);
      assertEquals(larger.memory().size(), 2_000_000_000);
      assertEquals(larger.memory().evictionType(), MEMORY);
   }

   public void testUseDefaultEviction() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      Configuration configuration = builder.build();

      assertFalse(configuration.memory().isEvictionEnabled());
      assertEquals(configuration.memory().storage(), HEAP);
      assertEquals(configuration.memory().storageType(), BINARY);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testPreventUsingLegacyAndNew() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().size(44).evictionType(COUNT);

      Configuration conf = builder.build();
      assertEquals(conf.memory().maxCount(), 44);

      builder.memory().maxCount(10).size(12);
      builder.build();
   }

   @Test
   public void testMinimal() {
      Configuration configuration = new ConfigurationBuilder().build();

      assertFalse(configuration.memory().isOffHeap());
      assertEquals(configuration.memory().maxCount(), -1L);
      assertEquals(configuration.memory().maxSizeBytes(), -1L);
      assertEquals(configuration.memory().storageType(), OBJECT);
   }

   @Test
   public void testChangeFromMinimal() {
      ConfigurationBuilder initial = new ConfigurationBuilder();
      Configuration initialConfig = initial.build();
      assertEquals(initialConfig.memory().storageType(), OBJECT);
      assertEquals(initialConfig.memory().size(), -1);

      initial.memory().size(3);
      Configuration larger = initial.build();
      assertEquals(larger.memory().storageType(), OBJECT);
      assertEquals(larger.memory().size(), 3);
      assertEquals(larger.memory().maxCount(), 3);
      assertEquals(larger.memory().storage(), HEAP);
   }

   @Test
   public void testRuntimeConfigChanges() {
      Configuration countBounded = new ConfigurationBuilder().memory().maxCount(1000).storage(OFF_HEAP).build();
      Configuration sizeBounded = new ConfigurationBuilder().memory().maxSize("10 MB").build();

      countBounded.memory().maxCount(1200);
      sizeBounded.memory().maxSize("20MB");

      assertEquals(countBounded.memory().maxCount(), 1200);
      assertEquals(sizeBounded.memory().maxSizeBytes(), 20_000_000);
      assertThrows(CacheException.class, () -> countBounded.memory().maxSize("30MB"));
      assertThrows(CacheException.class, () -> sizeBounded.memory().maxCount(2000));
   }

   @Test
   public void testParseLegacyXML() throws XMLStreamException {
      String xmlLegacy = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory>\n" +
            "            <object size=\"20000\" strategy=\"REMOVE\" />\n" +
            "         </memory>\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xmlLegacy);
      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlLegacy);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(afterParsing.memory().maxSizeBytes(), -1);
      assertEquals(afterParsing.memory().maxCount(), 20_000);
      assertEquals(afterParsing.memory().storageType(), OBJECT);
      assertEquals(afterParsing.memory().size(), 20_000);
      assertEquals(afterParsing.memory().evictionType(), COUNT);
   }

   @Test
   public void testParseLegacyXML2() throws XMLStreamException {
      String xmlLegacy = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory>\n" +
            "            <off-heap size=\"10000000\" eviction=\"MEMORY\" />\n" +
            "         </memory>\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xmlLegacy);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlLegacy);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(afterParsing.memory().maxSizeBytes(), 10_000_000);
      assertEquals(afterParsing.memory().maxCount(), -1);
      assertEquals(afterParsing.memory().storageType(), OFF_HEAP);
      assertEquals(afterParsing.memory().size(), 10_000_000);
      assertEquals(afterParsing.memory().evictionType(), MEMORY);
   }

   @Test
   public void testParseLegacyXML3() throws XMLStreamException {
      String xmlLegacy = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory>\n" +
            "            <binary size=\"-1\" />\n" +
            "         </memory>\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xmlLegacy);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlLegacy);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(afterParsing.memory().storage(), BINARY);
      assertEquals(afterParsing.memory().maxSizeBytes(), -1);
      assertEquals(afterParsing.memory().maxCount(), -1);
      assertEquals(afterParsing.memory().storageType(), BINARY);
      assertEquals(afterParsing.memory().size(), -1);
      assertEquals(afterParsing.memory().evictionType(), COUNT);

   }

   @Test
   public void testParseXML() throws XMLStreamException {
      String xml = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory storage=\"OFF_HEAP\" max-size=\"200 MB\" when-full=\"MANUAL\" />\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xml);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xml);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(afterParsing.memory().maxSizeBytes(), 200_000_000);
      assertEquals(afterParsing.memory().maxCount(), -1);
      assertEquals(afterParsing.memory().storageType(), OFF_HEAP);
      // Remove is forced
      assertEquals(afterParsing.memory().evictionStrategy(), REMOVE);
      assertEquals(afterParsing.memory().size(), 200_000_000);
      assertEquals(afterParsing.memory().evictionType(), MEMORY);
      assertEquals(afterParsing.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testParseXML2() throws XMLStreamException {
      String xmlNew = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory max-count=\"2000\" when-full=\"REMOVE\" />\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xmlNew);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlNew);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertEquals(afterParsing.memory().maxCount(), 2000);
      assertEquals(afterParsing.memory().whenFull(), REMOVE);

      assertEquals(afterParsing.memory().storageType(), OBJECT);
      assertEquals(afterParsing.memory().evictionStrategy(), REMOVE);
      assertEquals(afterParsing.memory().size(), 2000);
      assertEquals(afterParsing.memory().evictionType(), COUNT);
      assertEquals(afterParsing.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testParseXML3() throws XMLStreamException {
      String xmlNew = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <encoding media-type=\"application/json\" />\n" +
            "         <memory storage=\"HEAP\" max-size=\"1MB\" when-full=\"REMOVE\"/>\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      testSerializationAndBack(xmlNew);

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlNew);
      ConfigurationBuilder parsedBuilder = parsed.getNamedConfigurationBuilders().get("local");
      Configuration afterParsing = parsedBuilder.build();

      assertTrue(afterParsing.memory().isEvictionEnabled());
      assertEquals(afterParsing.memory().storage(), HEAP);
      assertEquals(afterParsing.memory().maxSizeBytes(), 1_000_000);
      assertEquals(afterParsing.memory().whenFull(), REMOVE);
      assertEquals(afterParsing.memory().storageType(), BINARY);
      assertEquals(afterParsing.memory().size(), 1_000_000);
      assertEquals(afterParsing.memory().evictionStrategy(), REMOVE);
      assertEquals(afterParsing.memory().evictionType(), MEMORY);
   }


   @Test
   public void testParseLegacyJSON() {
      String legacyJSON = "{\"local-cache\":{ \"memory\":{\"object\":{\"strategy\":\"REMOVE\",\"size\":5000}}}}";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JSON_READER.readJson(builder, legacyJSON);
      Configuration fromJson = builder.build();
      assertEquals(fromJson.memory().maxSizeBytes(), -1);
      assertEquals(fromJson.memory().maxCount(), 5000);
      assertEquals(fromJson.memory().storageType(), OBJECT);
      assertEquals(fromJson.memory().evictionStrategy(), REMOVE);
      assertEquals(fromJson.memory().size(), 5000);
      assertEquals(fromJson.memory().evictionType(), COUNT);
      assertEquals(fromJson.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testBuildWithLegacyConfiguration() {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.memory().storageType(OFF_HEAP).size(1_000).evictionType(COUNT);
      Configuration configuration = configBuilder.build();
      Configuration afterRead = new ConfigurationBuilder().read(configuration).build();

      assertEquals(afterRead.memory().storage(), OFF_HEAP);
      assertEquals(afterRead.memory().maxCount(), 1_000);
      assertEquals(afterRead.memory().maxSizeBytes(), -1);
      assertEquals(afterRead.memory().storageType(), OFF_HEAP);
      assertEquals(afterRead.memory().evictionStrategy(), REMOVE);
      assertEquals(afterRead.memory().size(), 1_000);
      assertEquals(afterRead.memory().evictionType(), COUNT);
      assertEquals(afterRead.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testBuildWithLegacyConfiguration2() {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.memory().storageType(HEAP).size(120);
      Configuration afterRead = new ConfigurationBuilder().read(configBuilder.build()).build();

      assertEquals(afterRead.memory().maxSizeBytes(), -1);
      assertEquals(afterRead.memory().maxCount(), 120);
      assertEquals(afterRead.memory().storageType(), HEAP);
      assertEquals(afterRead.memory().size(), 120);
      assertEquals(afterRead.memory().evictionType(), COUNT);

      ConfigurationBuilder override = new ConfigurationBuilder().read(afterRead);
      Configuration overridden = override.memory().size(400).build();
      assertEquals(overridden.memory().maxSizeBytes(), -1);
      assertEquals(overridden.memory().maxCount(), 400);
      assertEquals(overridden.memory().storageType(), HEAP);
      assertEquals(overridden.memory().size(), 400);
      assertEquals(overridden.memory().evictionType(), COUNT);
   }

   @Test
   public void testListenToLegacyAttribute() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storageType(HEAP).size(120);

      AtomicBoolean listenerTriggered = new AtomicBoolean();
      Configuration configuration = builder.build();
      configuration.memory().heapConfiguration().attributes().attribute(MemoryStorageConfiguration.SIZE)
            .addListener((attribute, oldValue) -> listenerTriggered.set(true));

      configuration.memory().size(400);

      assertEquals(400, configuration.memory().maxCount());
      assertEquals(400, configuration.memory().size());
      assertEquals(COUNT, configuration.memory().evictionType());
      assertTrue(listenerTriggered.get());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".*storage-type=OFF_HEAP, size=456, type=MEMORY have been deprecated and cannot be used in conjunction to the new configuration.*")
   public void testErrorForAmbiguousXML() {
      String xmlNew = "<infinispan>\n" +
            "   <cache-container>\n" +
            "      <local-cache name=\"local\">\n" +
            "         <memory max-size=\"1 MB\" when-full=\"MANUAL\">\n" +
            "            <off-heap size=\"456\" eviction=\"MEMORY\" />\n" +
            "         </memory>\n" +
            "      </local-cache>\n" +
            "   </cache-container>\n" +
            "</infinispan>\n";

      ConfigurationBuilderHolder parsed = new ParserRegistry().parse(xmlNew);
      parsed.getNamedConfigurationBuilders().get("local").build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".*Cannot configure both maxCount and maxSize.*")
   public void testErrorForMultipleThresholds() {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.memory().storage(OFF_HEAP).maxCount(10).maxSize("10TB").build();
   }

   private void testSerializationAndBack(String xml) throws XMLStreamException {
      // Parse config
      ConfigurationBuilderHolder configurationBuilderHolder = REGISTRY.parse(xml);
      ConfigurationBuilder builder = configurationBuilderHolder.getNamedConfigurationBuilders().get("local");
      Configuration before = builder.build();

      // Serialize the parsed config
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      REGISTRY.serialize(baos, "local", before);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ConfigurationBuilderHolder holderAfter = REGISTRY.parse(bais, null);

      // Parse again from the serialized
      ConfigurationBuilder afterParsing = holderAfter.getNamedConfigurationBuilders().get("local");

      Configuration after = afterParsing.build();
      assertEquals(after.memory(), before.memory());
   }
}
