package org.infinispan.configuration.module;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * ExtendedParserTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "unit", testName = "configuration.module.ExtendedParserTest")
public class ExtendedParserTest extends AbstractInfinispanTest {

   public void testExtendedParserBareExtension() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema("""
            <cache-container name="container-extra-modules" default-cache="extra-module">
               <local-cache name="extra-module">
                   <sample-element xmlns="urn:infinispan:config:mymodule" sample-attribute="test-value" />
               </local-cache>
            </cache-container>""");
      assertCacheConfiguration(config);
   }

   private void assertCacheConfiguration(String config) {
      ConfigurationBuilderHolder holder = parseToHolder(config);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)) {

         @Override
         public void call() {
            assertEquals("test-value", cm.getDefaultCacheConfiguration().module(MyModuleConfiguration.class).attribute());
         }

      });

   }

   private ConfigurationBuilderHolder parseToHolder(String config) {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      return parserRegistry.parse(config);
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "WRONG SCOPE")
   public void testExtendedParserWrongScope() {
      String config = TestingUtil.wrapXMLWithSchema("""
            <cache-container name="container-extra-modules" default-cache="extra-module">
               <local-cache name="extra-module">
               </local-cache>
               <sample-element xmlns="urn:infinispan:config:mymodule" sample-attribute="test-value" />
            </cache-container>""");
      parseToHolder(config);
   }
}
