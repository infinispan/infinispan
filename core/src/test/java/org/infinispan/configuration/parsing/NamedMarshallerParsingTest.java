package org.infinispan.configuration.parsing;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.NamedMarshallerConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshallerRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for parsing named marshaller XML configuration.
 *
 * @author William Burns
 * @since 16.2
 */
@Test(groups = "functional", testName = "configuration.parsing.NamedMarshallerParsingTest")
public class NamedMarshallerParsingTest extends AbstractInfinispanTest {

   public void testSingleNamedMarshaller() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller name="custom" marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      List<NamedMarshallerConfiguration> namedMarshallers = gc.serialization().namedMarshallers();
      assertEquals("Should have 1 named marshaller", 1, namedMarshallers.size());

      NamedMarshallerConfiguration config1 = namedMarshallers.get(0);
      assertEquals("custom", config1.name());
      assertEquals(JavaSerializationMarshaller.class.getName(), config1.marshallerClass());
   }

   public void testMultipleNamedMarshallers() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller name="marshaller1" marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller"/>
                        <named-marshaller name="marshaller2" marshaller="org.infinispan.marshall.TestObjectStreamMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      List<NamedMarshallerConfiguration> namedMarshallers = gc.serialization().namedMarshallers();
      assertEquals("Should have 2 named marshallers", 2, namedMarshallers.size());

      NamedMarshallerConfiguration config1 = namedMarshallers.get(0);
      assertEquals("marshaller1", config1.name());
      assertEquals(JavaSerializationMarshaller.class.getName(), config1.marshallerClass());

      NamedMarshallerConfiguration config2 = namedMarshallers.get(1);
      assertEquals("marshaller2", config2.name());
      assertEquals(TestObjectStreamMarshaller.class.getName(), config2.marshallerClass());
   }

   public void testNamedMarshallerWithDefaultMarshaller() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller">
                        <named-marshaller name="custom" marshaller="org.infinispan.marshall.TestObjectStreamMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      // Verify default marshaller
      assertNotNull(gc.serialization().marshaller());
      assertTrue(gc.serialization().marshaller() instanceof JavaSerializationMarshaller);

      // Verify named marshaller
      List<NamedMarshallerConfiguration> namedMarshallers = gc.serialization().namedMarshallers();
      assertEquals("Should have 1 named marshaller", 1, namedMarshallers.size());

      NamedMarshallerConfiguration config1 = namedMarshallers.get(0);
      assertEquals("custom", config1.name());
      assertEquals(TestObjectStreamMarshaller.class.getName(), config1.marshallerClass());
   }

   public void testNamedMarshallerWithContextInitializer() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <context-initializer class="org.infinispan.marshall.UserSCIImpl"/>
                        <named-marshaller name="custom" marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      ConfigurationBuilderHolder holder = parseStringConfiguration(config);
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();

      // Verify context initializers are present
      assertEquals(1, gc.serialization().contextInitializers().size());

      // Verify named marshaller
      List<NamedMarshallerConfiguration> namedMarshallers = gc.serialization().namedMarshallers();
      assertEquals("Should have 1 named marshaller", 1, namedMarshallers.size());

      NamedMarshallerConfiguration config1 = namedMarshallers.get(0);
      assertEquals("custom", config1.name());
      assertEquals(JavaSerializationMarshaller.class.getName(), config1.marshallerClass());
   }

   public void testNamedMarshallerIntegrationWithCacheManager() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller name="custom1" marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller"/>
                        <named-marshaller name="custom2" marshaller="org.infinispan.marshall.TestObjectStreamMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      try (InputStream is = new ByteArrayInputStream(config.getBytes())) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.fromStream(is);
         try {
            MarshallerRegistry registry = TestingUtil.extractGlobalComponent(cm, MarshallerRegistry.class);
            assertNotNull("MarshallerRegistry should not be null", registry);

            // Verify named marshallers are registered
            assertTrue("Should have custom1 marshaller", registry.hasMarshaller("custom1"));
            assertTrue("Should have custom2 marshaller", registry.hasMarshaller("custom2"));

            // Verify we can retrieve them
            assertNotNull("Should retrieve custom1 marshaller", registry.getMarshaller("custom1"));
            assertNotNull("Should retrieve custom2 marshaller", registry.getMarshaller("custom2"));

            // Verify types
            assertTrue("custom1 should be JavaSerializationMarshaller",
                  registry.getMarshaller("custom1") instanceof JavaSerializationMarshaller);
            assertTrue("custom2 should be TestObjectStreamMarshaller",
                  registry.getMarshaller("custom2") instanceof TestObjectStreamMarshaller);
         } finally {
            cm.stop();
         }
      }
   }

   public void testNamedMarshallerWithoutName() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller marshaller="org.infinispan.commons.marshall.JavaSerializationMarshaller"/>
                     </serialization>
                  </cache-container>"""
      );

      try {
         parseStringConfiguration(config);
         fail("Should have thrown an exception for missing name attribute");
      } catch (Exception e) {
         assertTrue("Should contain message about missing 'name' attribute",
               e.getMessage().contains("name") || e.getMessage().contains("Missing required attribute"));
      }
   }

   public void testNamedMarshallerWithoutMarshaller() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller name="custom"/>
                     </serialization>
                  </cache-container>"""
      );

      try {
         parseStringConfiguration(config);
         fail("Should have thrown an exception for missing marshaller attribute");
      } catch (Exception e) {
         assertTrue("Should contain message about missing 'marshaller' attribute",
               e.getMessage().contains("marshaller") || e.getMessage().contains("Missing required attribute"));
      }
   }

   public void testNamedMarshallerWithoutBothAttributes() {
      String config = TestingUtil.wrapXMLWithSchema(
            """
                  <cache-container>
                     <serialization>
                        <named-marshaller/>
                     </serialization>
                  </cache-container>"""
      );

      try {
         parseStringConfiguration(config);
         fail("Should have thrown an exception for missing both name and marshaller attributes");
      } catch (Exception e) {
         assertTrue("Should contain message about missing attributes",
               e.getMessage().contains("name") && e.getMessage().contains("marshaller") ||
               e.getMessage().contains("Missing required attribute"));
      }
   }

   // TODO: Add JSON/YAML configuration tests once proper list handling is implemented

   private static ConfigurationBuilderHolder parseStringConfiguration(String config) {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, System.getProperties());
      return parserRegistry.parse(is, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);
   }
}
