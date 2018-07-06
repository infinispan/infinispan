package org.infinispan.configuration.parsing;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.parsing.LegacyXmlFileParsingTest")
public class LegacyXmlFileParsingTest extends AbstractInfinispanTest {
   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testUnsupportedConfiguration() throws Exception {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configs/legacy/6.0.xml", true)) {

         @Override
         public void call() {
            fail("Parsing an unsupported file should have failed.");
         }

      });
   }
}
