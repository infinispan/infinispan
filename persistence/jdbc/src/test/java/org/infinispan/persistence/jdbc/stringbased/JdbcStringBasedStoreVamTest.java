package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.extractPersistenceMarshaller;

import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * JdbcStringBasedStoreTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreVamTest")
public class JdbcStringBasedStoreVamTest extends JdbcStringBasedStoreTest {

   EmbeddedCacheManager cm;
   StreamAwareMarshaller marshaller;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new JdbcStringBasedStoreVamTest().segmented(false),
            new JdbcStringBasedStoreVamTest().segmented(true),
      };
   }

   @BeforeClass
   public void setUpClass() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      marshaller = extractPersistenceMarshaller(cm.getCache().getCacheManager());
   }

   @AfterClass
   public void tearDownClass() throws PersistenceException {
      cm.stop();
   }

   @Override
   protected StreamAwareMarshaller getMarshaller() {
      return marshaller;
   }

}
