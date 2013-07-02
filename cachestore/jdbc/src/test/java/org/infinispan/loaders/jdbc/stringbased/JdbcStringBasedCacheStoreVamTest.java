package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * JdbcStringBasedCacheStoreTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreVamTest")
public class JdbcStringBasedCacheStoreVamTest extends JdbcStringBasedCacheStoreTest {

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
