package org.infinispan.configuration.module;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * ExtendedParserTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "unit", testName = "configuration.module.ExtendedParserTest")
public class ExtendedParserTest extends AbstractInfinispanTest {

   public void testExtendedParserModulesElement() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema("8.2",
            "<cache-container name=\"container-extra-modules\" default-cache=\"extra-module\">" +
            "   <local-cache name=\"extra-module\">\n" +
            "     <modules>\n" +
            "       <sample-element xmlns=\"urn:infinispan:config:mymodule\" sample-attribute=\"test-value\" />\n" +
            "     </modules>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      assertCacheConfiguration(config);
   }

   public void testExtendedParserBareExtension() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container name=\"container-extra-modules\" default-cache=\"extra-module\">" +
            "   <local-cache name=\"extra-module\">\n" +
            "       <sample-element xmlns=\"urn:infinispan:config:mymodule\" sample-attribute=\"test-value\" />\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      assertCacheConfiguration(config);
   }

   private void assertCacheConfiguration(String config) throws IOException {
      ConfigurationBuilderHolder holder = parseToHolder(config);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)) {

         @Override
         public void call() {
            Assert.assertEquals(cm.getDefaultCacheConfiguration().module(MyModuleConfiguration.class).attribute(), "test-value");
         }

      });

   }

   private ConfigurationBuilderHolder parseToHolder(String config) {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      return parserRegistry.parse(is);
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalStateException: WRONG SCOPE")
   public void testExtendedParserWrongScope() throws IOException {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container name=\"container-extra-modules\" default-cache=\"extra-module\">" +
            "   <local-cache name=\"extra-module\">\n" +
            "   </local-cache>\n" +
            "   <sample-element xmlns=\"urn:infinispan:config:mymodule\" sample-attribute=\"test-value\" />\n" +
            "</cache-container>"
      );
      parseToHolder(config);
   }
}
