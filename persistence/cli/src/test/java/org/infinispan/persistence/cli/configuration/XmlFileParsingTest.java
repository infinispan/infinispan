package org.infinispan.persistence.cli.configuration;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "persistence.remote.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <cli-loader xmlns=\"urn:infinispan:config:store:cli:7.0\" " +
            "                  connection=\"jmx://1.2.3.4:4444/MyCacheManager/myCache\">\n" +
            "       </cli-loader>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      CLInterfaceLoaderConfiguration store = (CLInterfaceLoaderConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals("jmx://1.2.3.4:4444/MyCacheManager/myCache", store.connectionString());
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().persistence().stores().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}