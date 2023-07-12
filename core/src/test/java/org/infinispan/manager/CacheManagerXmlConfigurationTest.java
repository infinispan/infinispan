package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.security.Security;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerXmlConfigurationTest")
public class CacheManagerXmlConfigurationTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;
   private Subject KING = TestingUtil.makeSubject("king-arthur", "king");

   @AfterMethod
   public void tearDown() {
      if (cm != null)
         Security.doAs(KING,  () -> cm.stop());
      cm = null;
   }

   public void testNamedCacheXML() throws IOException {
      cm = TestCacheManagerFactory.fromXml("configs/named-cache-test.xml", false, false, new TransportFlags());

      GlobalConfiguration globalConfiguration = TestingUtil.extractGlobalConfiguration(cm);
      assertEquals("s1", globalConfiguration.transport().siteId());
      assertEquals("r1", globalConfiguration.transport().rackId());
      assertEquals("m1", globalConfiguration.transport().machineId());
      assertNotNull(globalConfiguration.transport().transport());

      // test default cache
      Configuration c = getDefaultCacheConfiguration();
      assertEquals(100, c.locking().concurrencyLevel());
      assertEquals(1000, c.locking().lockAcquisitionTimeout());
      assertFalse(c.transaction().transactionMode().isTransactional());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, c.transaction().transactionMode());

      // test the "transactional" cache
      c = getCacheConfiguration(cm, "transactional");
      assertTrue(c.transaction().transactionMode().isTransactional());
      assertEquals(32, c.locking().concurrencyLevel());
      assertEquals(10000, c.locking().lockAcquisitionTimeout());
      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);

      // test the "replicated" cache
      c = getCacheConfiguration(cm, "syncRepl");
      assertEquals(32, c.locking().concurrencyLevel());
      assertEquals(10000, c.locking().lockAcquisitionTimeout());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, c.transaction().transactionMode());

      // test the "txSyncRepl" cache
      c = getCacheConfiguration(cm, "txSyncRepl");
      assertEquals(32, c.locking().concurrencyLevel());
      assertEquals(10000, c.locking().lockAcquisitionTimeout());
      assertTrue(c.transaction().transactionManagerLookup() instanceof GenericTransactionManagerLookup);
   }

   private Configuration getCacheConfiguration(EmbeddedCacheManager cm, String cacheName) {
      return Security.doAs(KING, () -> cm.getCacheConfiguration(cacheName));
   }

   private Configuration getDefaultCacheConfiguration() {
      return Security.doAs(KING, () -> cm.getDefaultCacheConfiguration());
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
         Configuration c = cm.getCacheConfiguration("default");
         assertTrue(c.invocationBatching().enabled());
         assertTrue(c.transaction().transactionMode().isTransactional());

         c = cm.getDefaultCacheConfiguration();
         assertTrue(c.invocationBatching().enabled());

         Configuration c2 = cm.getCacheConfiguration("tml");
         assertTrue(c2.transaction().transactionMode().isTransactional());
      } finally {
         cm.stop();
      }
   }

   public void testXInclude() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/include.xml");
      try {
         assertEquals("included", cm.getCacheManagerConfiguration().defaultCacheName().get());
         assertNotNull(cm.getCacheConfiguration("included"));
         assertEquals(CacheMode.LOCAL, cm.getCacheConfiguration("included").clustering().cacheMode());
         assertEquals(CacheMode.LOCAL, cm.getCacheConfiguration("another-included").clustering().cacheMode());
      } finally {
         cm.stop();
      }
   }
}
