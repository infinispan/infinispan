package org.infinispan.configuration.module;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil.InfinispanStartTag;
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

   public void testExtendedParser() throws IOException {
      String config = InfinispanStartTag.LATEST +
            "<cache-container name=\"container-extra-modules\" default-cache=\"extra-module\">" +
            "   <local-cache name=\"extra-module\">\n" +
            "     <modules>\n" +
            "       <sample-element xmlns=\"urn:infinispan:config:mymodule\" sample-attribute=\"test-value\" />\n" +
            "     </modules>\n" +
            "   </local-cache>\n" +
            "</cache-container>" +
            INFINISPAN_END_TAG;
      assertCacheConfiguration(config);
   }

   private void assertCacheConfiguration(String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parse(is);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager(holder)) {

         @Override
         public void call() {
            Assert.assertEquals(cm.getDefaultCacheConfiguration().module(MyModuleConfiguration.class).attribute(), "test-value");
         }

      });

   }
}
