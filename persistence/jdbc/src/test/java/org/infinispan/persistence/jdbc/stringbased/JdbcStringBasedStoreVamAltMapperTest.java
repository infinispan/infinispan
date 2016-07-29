package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;

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

   @BeforeClass
   @Override
   public void createCacheStore() throws PersistenceException {
      cm = TestCacheManagerFactory.createCacheManager(false);
      marshaller = extractGlobalMarshaller(cm);

      super.createCacheStore();
   }

   @AfterClass
   @Override
   public void destroyStore() throws PersistenceException {
      super.destroyStore();
      cm.stop();
   }

   protected StreamingMarshaller getMarshaller() {
      return marshaller;
   }

}
