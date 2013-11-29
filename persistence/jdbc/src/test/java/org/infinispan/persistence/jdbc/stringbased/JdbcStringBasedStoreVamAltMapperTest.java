package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * JdbcStringBasedStoreAltMapperTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreVamAltMapperTest")
public class JdbcStringBasedStoreVamAltMapperTest extends JdbcStringBasedStoreAltMapperTest {

   EmbeddedCacheManager cm;
   StreamingMarshaller marshaller;

   @BeforeTest
   @Override
   public void createCacheStore() throws PersistenceException {
      cm = TestCacheManagerFactory.createCacheManager(false);
      marshaller = extractCacheMarshaller(cm.getCache());

      super.createCacheStore();
   }

   @AfterTest
   @Override
   public void destroyStore() throws PersistenceException {
      super.destroyStore();
      cm.stop();
   }

   protected StreamingMarshaller getMarshaller() {
      return marshaller;
   }

}
