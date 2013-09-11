package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * JdbcStringBasedCacheStoreAltMapperTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedCacheStoreVamAltMapperTest")
public class JdbcStringBasedCacheStoreVamAltMapperTest extends JdbcStringBasedCacheStoreAltMapperTest {

   EmbeddedCacheManager cm;
   StreamingMarshaller marshaller;


   @BeforeTest
   @Override
   public void createCacheStore() throws CacheLoaderException {
      cm = TestCacheManagerFactory.createCacheManager(false);
      marshaller = extractCacheMarshaller(cm.getCache());

      super.createCacheStore();
   }

   @AfterTest
   @Override
   public void destroyStore() throws CacheLoaderException {
      super.destroyStore();

      cm.stop();
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return marshaller;
   }

}
