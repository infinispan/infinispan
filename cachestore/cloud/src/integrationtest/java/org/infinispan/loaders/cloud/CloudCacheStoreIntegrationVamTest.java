package org.infinispan.loaders.cloud;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * CloudCacheStoreIntegrationTest using production level marshaller.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "unit", sequential = true, testName = "loaders.cloud.CloudCacheStoreIntegrationVamTest")
public class CloudCacheStoreIntegrationVamTest extends CloudCacheStoreIntegrationTest {

   EmbeddedCacheManager cm;
   StreamingMarshaller marshaller;

   @BeforeClass
   public void setUpClass() {
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
      marshaller = extractCacheMarshaller(cm.getCache());
   }

   @AfterClass
   public void tearDownClass() throws CacheLoaderException {
      cm.stop();
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return marshaller;
   }

}
