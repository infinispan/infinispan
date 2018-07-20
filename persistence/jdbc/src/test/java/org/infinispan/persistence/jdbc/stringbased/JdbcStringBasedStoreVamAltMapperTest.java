package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.extractPersistenceMarshaller;

import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * JdbcStringBasedStoreAltMapperTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreVamAltMapperTest")
public class JdbcStringBasedStoreVamAltMapperTest extends JdbcStringBasedStoreAltMapperTest {

   EmbeddedCacheManager cm;
   StreamAwareMarshaller marshaller;

   @BeforeClass
   @Override
   public void createCacheStore() throws PersistenceException {
      cm = TestCacheManagerFactory.createCacheManager(false);
      marshaller = extractPersistenceMarshaller(cm);

      super.createCacheStore();
   }

   @AfterClass
   @Override
   public void destroyStore() throws PersistenceException {
      super.destroyStore();
      cm.stop();
   }

   protected StreamAwareMarshaller getMarshaller() {
      return marshaller;
   }

}
