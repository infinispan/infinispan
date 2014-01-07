package org.infinispan.manager;

import static org.testng.AssertJUnit.assertFalse;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.testng.Assert.*;

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
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "transactional" cache
      c = cm.getCache("transactional");
      assert c.getCacheConfiguration().transaction().transactionMode().isTransactional();
      assert c.getCacheConfiguration().locking().concurrencyLevel() == 100;
      assert c.getCacheConfiguration().locking().lockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "replicated" cache
      c = cm.getCache("syncRepl");
      assert c.getCacheConfiguration().locking().concurrencyLevel() == 100;
      assert c.getCacheConfiguration().locking().lockAcquisitionTimeout() == 1000;
      assertEquals(TransactionMode.NON_TRANSACTIONAL, c.getCacheConfiguration().transaction().transactionMode());
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";

      // test the "txSyncRepl" cache
      c = cm.getCache("txSyncRepl");
      assert c.getCacheConfiguration().locking().concurrencyLevel() == 100;
      assert c.getCacheConfiguration().locking().lockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) != null : "This should not be null, since a shared transport should be present";
   }

   public void testNamedCacheXMLClashingNames() {
      String xml = INFINISPAN_START_TAG +
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
            "</cache-container>" +
            INFINISPAN_END_TAG;

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      try {
         cm = TestCacheManagerFactory.fromStream(bais);
         assert false : "Should fail";
      } catch (Throwable expected) {
      }
   }

   public void testNamedCacheXMLClashingNamesProgrammatic() throws IOException {
      String xml = INFINISPAN_START_TAG +
            "\n" +
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "        <locking concurrency-level=\"100\" acquire-timeout=\"1000\" />\n" +
            "    </local-cache>\n" +
            "\n" +
            "   <local-cache name=\"c1\">\n" +
            "        <transaction transaction-manager-lookup=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"/>\n" +
            "    </local-cache>\n" +
            "</cache-container>" +
            INFINISPAN_END_TAG;

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      cm = TestCacheManagerFactory.fromStream(bais);

      assertNotNull(cm.getCache());
      assertNotNull(cm.getCache("c1"));
      Configuration c1Config = cm.getCache("c1").getCacheConfiguration();
      assertNotNull(c1Config);
      Configuration redefinedConfig = cm.defineConfiguration("c1", new ConfigurationBuilder().read(c1Config).build());
      assertEquals(c1Config, redefinedConfig);
   }

   public void testBatchingIsEnabled() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.fromXml("configs/batching.xml");
      try {
         Cache c = cm.getCache("any");
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


