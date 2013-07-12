package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * JdbcMixedCacheStore2Test using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreVam2Test")
public class JdbcMixedCacheStoreVam2Test extends JdbcMixedCacheStore2Test {

   EmbeddedCacheManager cm;
   StreamingMarshaller marshaller;

   @BeforeClass
   public void setUpClass() {
      cm = TestCacheManagerFactory.createCacheManager(false);
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
