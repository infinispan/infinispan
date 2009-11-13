package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.rpc.RpcManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerXmlConfigurationTest")
public class CacheManagerXmlConfigurationTest extends AbstractInfinispanTest {
   DefaultCacheManager cm;

   @AfterMethod
   public void tearDown() {
      if (cm != null) cm.stop();
      cm =null;
   }

   public void testNamedCacheXML() throws IOException {
      cm = new DefaultCacheManager("configs/named-cache-test.xml");

      // test default cache
      Cache c = cm.getCache();
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(c, RpcManager.class) != null : "This should not be null, since a shared RPC manager should be present";

      // test the "transactional" cache
      c = cm.getCache("transactional");
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, RpcManager.class) != null : "This should not be null, since a shared RPC manager should be present";

      // test the "replicated" cache
      c = cm.getCache("syncRepl");
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(c, RpcManager.class) != null : "This should not be null, since a shared RPC manager should be present";

      // test the "txSyncRepl" cache
      c = cm.getCache("txSyncRepl");
      assert c.getConfiguration().getConcurrencyLevel() == 100;
      assert c.getConfiguration().getLockAcquisitionTimeout() == 1000;
      assert TestingUtil.extractComponent(c, TransactionManager.class) != null;
      assert TestingUtil.extractComponent(c, RpcManager.class) != null : "This should not be null, since a shared RPC manager should be present";
   }

   public void testNamedCacheXMLClashingNames() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"100\" lockAcquisitionTimeout=\"1000\" />\n" +
            "    </default>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <transaction transactionManagerLookupClass=\"org.infinispan.transaction.GenericTransactionManagerLookup\"/>\n" +
            "    </namedCache>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <clustering>\n" +
            "            <sync replTimeout=\"15000\"/>\n" +
            "        </clustering>\n" +
            "    </namedCache>\n" +
            "    \n" +
            "</infinispan>";

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      try {
         cm = new DefaultCacheManager(bais);
         assert false : "Should fail";
      } catch (Throwable expected) {

         System.out.println("Blah");

      }
   }

   public void testNamedCacheXMLClashingNamesProgrammatic() throws IOException {
      String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\">\n" +
            "\n" +
            "<global/>\n" +
            "    <default>\n" +
            "        <locking concurrencyLevel=\"100\" lockAcquisitionTimeout=\"1000\" />\n" +
            "    </default>\n" +
            "\n" +
            "    <namedCache name=\"c1\">\n" +
            "        <transaction transactionManagerLookupClass=\"org.infinispan.transaction.lookup.GenericTransactionManagerLookup\"/>\n" +
            "    </namedCache>\n" +
            "\n" +            
            "\n" +
            "</infinispan>";

      ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes());
      cm = new DefaultCacheManager(bais);

      assert cm.getCache() != null;
      assert cm.getCache("c1") != null;
      Configuration c1Config = cm.getCache("c1").getConfiguration();
      assert c1Config != null;
      Configuration redefinedConfig = cm.defineConfiguration("c1", new Configuration());
      assert c1Config.equals(redefinedConfig);
   }
}


