package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerXmlConfigurationTest")
public class CacheManagerXmlConfigurationTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;

   @AfterMethod
   public void tearDown() {
      if (cm != null) cm.stop();
      cm =null;
   }

   public void testNamedCacheXML() throws IOException {
      cm = TestCacheManagerFactory.fromXml("configs/named-cache-test.xml");

      assertEquals("s1", cm.getCacheManagerConfiguration().transport().siteId());
      assertEquals("r1", cm.getCacheManagerConfiguration().transport().rackId());
      assertEquals("m1", cm.getCacheManagerConfiguration().transport().machineId());

      // test default cache
      Cache c = cm.getCache();
      assertEquals(100, c.getCacheConfiguration().locking().concurrencyLevel());
      assertEquals(1000, c.getCacheConfiguration().locking().lockAcquisitionTimeout());
      assertFalse(c.getCacheConfiguration().transaction().transactionMode().isTransactional());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, c.getCacheConfiguration().transaction().transactionMode());
      assertNotNull("This should not be null, since a shared transport should be present", TestingUtil.extractComponent(c, Transport.class));

      // test the "transactional" cache
      c = cm.getCache("transactional");
      assertTrue(c.getCacheConfiguration().transaction().transactionMode().isTransactional());
      assertEquals(32, c.getCacheConfiguration().locking().concurrencyLevel());
      assertEquals(10000, c.getCacheConfiguration().locking().lockAcquisitionTimeout());
      assertNotNull(TestingUtil.extractComponent(c, TransactionManager.class));
      assertNotNull("This should not be null, since a shared transport should be present", TestingUtil.extractComponent(c, Transport.class));

      // test the "replicated" cache
      c = cm.getCache("syncRepl");
      assertEquals(32, c.getCacheConfiguration().locking().concurrencyLevel());
      assertEquals(10000, c.getCacheConfiguration().locking().lockAcquisitionTimeout());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, c.getCacheConfiguration().transaction().transactionMode());
      assertNotNull("This should not be null, since a shared transport should be present", TestingUtil.extractComponent(c, Transport.class));

      // test the "txSyncRepl" cache
      c = cm.getCache("txSyncRepl");
      assertEquals(32, c.getCacheConfiguration().locking().concurrencyLevel());
      assertEquals(10000, c.getCacheConfiguration().locking().lockAcquisitionTimeout());
      assertNotNull(TestingUtil.extractComponent(c, TransactionManager.class));
      assertNotNull("This should not be null, since a shared transport should be present", TestingUtil.extractComponent(c, Transport.class));
   }

   public void testNamedCacheXMLClashingNames() {
      String xml = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "\n" +
            "   <local-cache name=\"default\">\n" +
            "        <locking concurrencyLevel=\"100\" lockAcquisitionTimeout=\"1000\" />\n" +
            "   </local-cache>\n" +
            "\n" +
            "   <local-cache name=\"c1\">\n" +
            "        <transaction transaction-manager-lookup=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"/>\n" +
            "   </local-cache>\n" +
            "\n" +
            "   <replicated-cache name=\"c1\" mode=\"SYNC\" remote-timeout=\"15000\">\n" +
            "   </replicated-cache>\n" +
            "</cache-container>");

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      try {
         cm = TestCacheManagerFactory.fromStream(bais);
         assert false : "Should fail";
      } catch (Throwable expected) {
      }
   }

   public void testBatchingIsEnabled() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/batching.xml");
      try {
         Cache c = cm.getCache("default");
         assertTrue(c.getCacheConfiguration().invocationBatching().enabled());
         assertTrue(c.getCacheConfiguration().transaction().transactionMode().isTransactional());
         c = cm.getCache();
         assertTrue(c.getCacheConfiguration().invocationBatching().enabled());
         Cache c2 = cm.getCache("tml");
         assertTrue(c2.getCacheConfiguration().transaction().transactionMode().isTransactional());
      } finally {
         cm.stop();
      }
   }

}
