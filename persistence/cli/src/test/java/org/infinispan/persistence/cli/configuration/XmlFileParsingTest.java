package org.infinispan.persistence.cli.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.Version;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.remote.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "   <cache-container default-cache=\"default\">\n" +
            "      <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <cli-loader xmlns=\"urn:infinispan:config:store:cli:"+ Version.getSchemaVersion() + "\" " +
            "                  connection=\"jmx://1.2.3.4:4444/MyCacheManager/myCache\">\n" +
            "       </cli-loader>\n" +
            "     </persistence>\n" +
            "   </local-cache></cache-container>"
      );

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
