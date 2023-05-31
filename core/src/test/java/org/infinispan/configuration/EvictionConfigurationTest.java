package org.infinispan.configuration;

import static org.infinispan.configuration.cache.StorageType.BINARY;
import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.configuration.cache.StorageType.OBJECT;
import static org.infinispan.configuration.cache.StorageType.OFF_HEAP;
import static org.infinispan.eviction.EvictionStrategy.REMOVE;
import static org.infinispan.eviction.EvictionType.COUNT;
import static org.infinispan.eviction.EvictionType.MEMORY;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Queue;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfiguration;
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

   @Test
   public void testReuseLegacyBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().size(200);

      Configuration configuration = builder.build();

      assertEquals(configuration.memory().maxSizeBytes(), -1);
      assertEquals(configuration.memory().maxCount(), 200);
      assertEquals(configuration.memory().storageType(), HEAP);

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
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      Configuration configuration = builder.build();

      assertEquals(configuration.memory().maxSizeBytes(), 1_500_000_000);
      assertEquals(configuration.memory().maxCount(), -1);
      assertEquals(configuration.memory().whenFull(), REMOVE);
      assertEquals(configuration.memory().storageType(), HEAP);
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
      assertEquals(larger.memory().storageType(), HEAP);
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
      assertEquals(configuration.memory().storageType(), HEAP);
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
      assertEquals(configuration.memory().storageType(), HEAP);
   }

   @Test
   public void testChangeFromMinimal() {
      ConfigurationBuilder initial = new ConfigurationBuilder();
      Configuration initialConfig = initial.build();
      assertEquals(initialConfig.memory().storageType(), HEAP);
      assertEquals(initialConfig.memory().size(), -1);

      initial.memory().size(3);
      Configuration larger = initial.build();
      assertEquals(larger.memory().storageType(), HEAP);
      assertEquals(larger.memory().size(), 3);
      assertEquals(larger.memory().maxCount(), 3);
      assertEquals(larger.memory().storage(), HEAP);
   }

   @Test
   public void testRuntimeConfigChanges() {
      Configuration countBounded = new ConfigurationBuilder().memory().maxCount(1000).build();
      Configuration sizeBounded = new ConfigurationBuilder().memory().maxSize("10 MB").storage(OFF_HEAP).build();

      countBounded.memory().maxCount(1200);
      sizeBounded.memory().maxSize("20MB");

      assertEquals(countBounded.memory().maxCount(), 1200);
      assertEquals(sizeBounded.memory().maxSizeBytes(), 20_000_000);
      Exceptions.expectException(CacheConfigurationException.class, () -> countBounded.memory().maxSize("30MB"));
      Exceptions.expectException(CacheConfigurationException.class, () -> sizeBounded.memory().maxCount(2000));
   }

   @Test
   public void testParseXML() {
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
   public void testParseXML2() {
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

      assertEquals(afterParsing.memory().storageType(), HEAP);
      assertEquals(afterParsing.memory().evictionStrategy(), REMOVE);
      assertEquals(afterParsing.memory().size(), 2000);
      assertEquals(afterParsing.memory().evictionType(), COUNT);
      assertEquals(afterParsing.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testParseXML3() {
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
      assertEquals(afterParsing.memory().storageType(), HEAP);
      assertEquals(afterParsing.memory().size(), 1_000_000);
      assertEquals(afterParsing.memory().evictionStrategy(), REMOVE);
      assertEquals(afterParsing.memory().evictionType(), MEMORY);
   }


   @Test
   public void testParseJSON() {
      ConfigurationBuilderHolder holder = new ParserRegistry().parse("{\"local-cache\":{ \"memory\":{\"storage\":\"HEAP\",\"when-full\":\"REMOVE\",\"max-count\":5000}}}}", MediaType.APPLICATION_JSON);
      Configuration fromJson = holder.getCurrentConfigurationBuilder().build();
      assertEquals(fromJson.memory().maxSizeBytes(), -1);
      assertEquals(fromJson.memory().maxCount(), 5000);
      assertEquals(fromJson.memory().storageType(), HEAP);
      assertEquals(fromJson.memory().evictionStrategy(), REMOVE);
      assertEquals(fromJson.memory().size(), 5000);
      assertEquals(fromJson.memory().evictionType(), COUNT);
      assertEquals(fromJson.memory().heapConfiguration().evictionStrategy(), REMOVE);
   }

   @Test
   public void testParseLegacyJSON() {
      ConfigurationBuilderHolder holder = new ParserRegistry().parse("{\"local-cache\":{ \"memory\":{\"storage\":\"OBJECT\", \"when-full\":\"REMOVE\",\"max-count\":5000}}}", MediaType.APPLICATION_JSON);
      Configuration fromJson = holder.getCurrentConfigurationBuilder().build();
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
      Configuration afterRead = new ConfigurationBuilder().read(configuration, Combine.DEFAULT).build();

      assertEquals(OFF_HEAP, afterRead.memory().storage());
      assertEquals(REMOVE, afterRead.memory().whenFull());
      assertEquals(1_000, afterRead.memory().maxCount());
      assertEquals(-1, afterRead.memory().maxSizeBytes());
      assertEquals(OFF_HEAP, afterRead.memory().storageType());
      assertEquals(REMOVE, afterRead.memory().evictionStrategy());
      assertEquals(1_000, afterRead.memory().size());
      assertEquals(COUNT, afterRead.memory().evictionType());
      assertEquals(REMOVE, afterRead.memory().heapConfiguration().evictionStrategy());
   }

   @Test
   public void testBuildWithLegacyConfiguration2() {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder.memory().storageType(HEAP).size(120);
      Configuration afterRead = new ConfigurationBuilder().read(configBuilder.build(), Combine.DEFAULT).build();

      assertEquals(afterRead.memory().maxSizeBytes(), -1);
      assertEquals(afterRead.memory().maxCount(), 120);
      assertEquals(afterRead.memory().storageType(), HEAP);
      assertEquals(afterRead.memory().size(), 120);
      assertEquals(afterRead.memory().evictionType(), COUNT);

      ConfigurationBuilder override = new ConfigurationBuilder().read(afterRead, Combine.DEFAULT);
      Configuration overridden = override.memory().size(400).build();
      assertEquals(overridden.memory().maxSizeBytes(), -1);
      assertEquals(overridden.memory().maxCount(), 400);
      assertEquals(overridden.memory().storageType(), HEAP);
      assertEquals(overridden.memory().size(), 400);
      assertEquals(overridden.memory().evictionType(), COUNT);
   }

   public void testListenToCountChanges() {
      ConfigurationBuilder countBuilder = new ConfigurationBuilder();
      countBuilder.memory().storage(HEAP).maxCount(20);

      Configuration configuration = countBuilder.build();
      assertEquals(COUNT, configuration.memory().evictionType());

      Queue<Object> sizeListenerQueue = new ArrayDeque<>(1);
      Queue<Object> maxCountListenerQueue = new ArrayDeque<>(1);
      Queue<Object> maxSizeListenerQueue = new ArrayDeque<>(1);
      setUpListeners(configuration, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().size(100);
      assertCountUpdate(configuration, 100, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().heapConfiguration().size(200);
      assertCountUpdate(configuration, 200, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().maxCount(300);
      assertCountUpdate(configuration, 300, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);
   }

   public void testListenToSizeChanges() {
      ConfigurationBuilder sizeBuilder = new ConfigurationBuilder();
      sizeBuilder.memory().storage(HEAP).maxSize("20");
      sizeBuilder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      Configuration configuration = sizeBuilder.build();
      assertEquals(MEMORY, configuration.memory().evictionType());

      Queue<Object> sizeListenerQueue = new ArrayDeque<>(1);
      Queue<Object> maxSizeListenerQueue = new ArrayDeque<>(1);
      Queue<Object> maxCountListenerQueue = new ArrayDeque<>(1);
      setUpListeners(configuration, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().size(100);
      assertSizeUpdate(configuration, 100, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().heapConfiguration().size(200);
      assertSizeUpdate(configuration, 200, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);

      configuration.memory().maxSize("300");
      assertSizeUpdate(configuration, 300, sizeListenerQueue, maxCountListenerQueue, maxSizeListenerQueue);
   }

   private void setUpListeners(Configuration configuration, Queue<Object> sizeListenerQueue,
                               Queue<Object> maxCountListenerQueue, Queue<Object> maxSizeListenerQueue) {
      configuration.memory().heapConfiguration().attributes().attribute(MemoryStorageConfiguration.SIZE)
                   .addListener((attribute, oldValue) -> sizeListenerQueue.add(attribute.get()));
      configuration.memory().attributes().attribute(MemoryConfiguration.MAX_COUNT)
                   .addListener((attribute, oldValue) -> maxCountListenerQueue.add(attribute.get()));
      configuration.memory().attributes().attribute(MemoryConfiguration.MAX_SIZE)
                   .addListener((attribute, oldValue) -> maxSizeListenerQueue.add(attribute.get()));
   }

   private void assertCountUpdate(Configuration configuration, long newValue, Queue<Object> sizeListenerQueue,
                                 Queue<Object> maxCountListenerQueue, Queue<Object> maxSizeListenerQueue) {
      assertEquals(newValue, configuration.memory().size());
      assertEquals(newValue, sizeListenerQueue.poll());

      assertEquals(newValue, configuration.memory().maxCount());
      assertEquals(newValue, maxCountListenerQueue.poll());

      assertEquals(-1L, configuration.memory().maxSizeBytes());
      assertEquals(0, maxSizeListenerQueue.size());
   }

   private void assertSizeUpdate(Configuration configuration, long newValue, Queue<Object> sizeListenerQueue,
                                 Queue<Object> maxCountListenerQueue, Queue<Object> maxSizeListenerQueue) {
      assertEquals(newValue, configuration.memory().size());
      assertEquals(newValue, sizeListenerQueue.poll());

      assertEquals(String.valueOf(newValue), configuration.memory().maxSize());
      assertEquals(newValue, configuration.memory().maxSizeBytes());
      assertEquals(String.valueOf(newValue), maxSizeListenerQueue.poll());

      assertEquals(-1L, configuration.memory().maxCount());
      assertEquals(0, maxCountListenerQueue.size());
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
      REGISTRY.serialize(baos, "local", before);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ConfigurationBuilderHolder holderAfter = REGISTRY.parse(bais, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);

      // Parse again from the serialized
      ConfigurationBuilder afterParsing = holderAfter.getNamedConfigurationBuilders().get("local");

      Configuration after = afterParsing.build();
      assertEquals(after.memory(), before.memory());
   }
}
