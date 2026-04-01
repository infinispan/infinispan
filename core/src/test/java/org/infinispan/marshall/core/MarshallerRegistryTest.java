package org.infinispan.marshall.core;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for {@link MarshallerRegistry}.
 *
 * @author William Burns
 * @since 16.2
 */
@Test(groups = "functional", testName = "marshall.core.MarshallerRegistryTest")
public class MarshallerRegistryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();

      // Add a named marshaller by class name
      gcb.serialization()
            .addNamedMarshaller("testMarshaller", JavaSerializationMarshaller.class.getName());

      // Add a named marshaller by instance
      gcb.serialization()
            .addNamedMarshaller("instanceMarshaller", new JavaSerializationMarshaller());

      return TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder());
   }

   public void testGetDefaultMarshaller() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);
      assertNotNull("MarshallerRegistry should not be null", registry);

      Marshaller defaultMarshaller = registry.getDefaultMarshaller();
      assertNotNull("Default marshaller should not be null", defaultMarshaller);

      Marshaller explicitDefault = registry.getMarshaller(MarshallerRegistry.DEFAULT_MARSHALLER_NAME);
      assertEquals("Default marshaller should be same as explicit default", defaultMarshaller, explicitDefault);
   }

   public void testGetNamedMarshallerByClassName() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);

      Marshaller marshaller = registry.getMarshaller("testMarshaller");
      assertNotNull("Named marshaller should not be null", marshaller);
      assertTrue("Named marshaller should be JavaSerializationMarshaller", marshaller instanceof JavaSerializationMarshaller);
   }

   public void testGetNamedMarshallerByInstance() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);

      Marshaller marshaller = registry.getMarshaller("instanceMarshaller");
      assertNotNull("Named marshaller should not be null", marshaller);
      assertTrue("Named marshaller should be JavaSerializationMarshaller", marshaller instanceof JavaSerializationMarshaller);
   }

   public void testHasMarshaller() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);

      assertTrue("Should have default marshaller", registry.hasMarshaller(MarshallerRegistry.DEFAULT_MARSHALLER_NAME));
      assertTrue("Should have testMarshaller", registry.hasMarshaller("testMarshaller"));
      assertTrue("Should have instanceMarshaller", registry.hasMarshaller("instanceMarshaller"));
      assertFalse("Should not have nonexistent marshaller", registry.hasMarshaller("nonexistent"));
   }

   public void testDuplicateMarshallerNameThrowsException() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();

      gcb.serialization()
            .addNamedMarshaller("duplicate", JavaSerializationMarshaller.class.getName());

      try {
         gcb.serialization()
               .addNamedMarshaller("duplicate", JavaSerializationMarshaller.class.getName());
         fail("Adding duplicate marshaller name should throw IllegalArgumentException");
      } catch (IllegalArgumentException e) {
         // Expected
         assertTrue(e.getMessage().contains("duplicate"));
      }
   }

   public void testBuilderApi() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();

      // Test adding via builder API
      gcb.serialization()
            .addNamedMarshaller()
            .name("builderTest")
            .marshallerClass(JavaSerializationMarshaller.class.getName());

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder());
      try {
         MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cm, MarshallerRegistry.class);
         assertNotNull("Named marshaller should exist", registry.getMarshaller("builderTest"));
      } finally {
         cm.stop();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testGetMarshallerWithNullName() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);
      registry.getMarshaller(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testGetMarshallerWithEmptyName() {
      MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cacheManager, MarshallerRegistry.class);
      registry.getMarshaller("");
   }

   public void testXmlConfiguration() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<infinispan xmlns=\"urn:infinispan:config:16.2\">\n" +
            "   <cache-container>\n" +
            "      <serialization>\n" +
            "         <named-marshaller name=\"xmlMarshaller\" marshaller=\"org.infinispan.commons.marshall.JavaSerializationMarshaller\"/>\n" +
            "      </serialization>\n" +
            "   </cache-container>\n" +
            "</infinispan>";

      try (InputStream is = new ByteArrayInputStream(xml.getBytes())) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(is);
         try {
            MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cm, MarshallerRegistry.class);
            assertNotNull("Named marshaller from XML should exist", registry.getMarshaller("xmlMarshaller"));
            assertTrue("Named marshaller should be JavaSerializationMarshaller",
                  registry.getMarshaller("xmlMarshaller") instanceof JavaSerializationMarshaller);
         } finally {
            cm.stop();
         }
      }
   }
}
