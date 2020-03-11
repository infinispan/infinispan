package org.infinispan.persistence.remote.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.remote.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <remote-store xmlns=\"urn:infinispan:config:store:remote:"+ Version.getSchemaVersion() + "\" >\n" +
            "         <remote-server host=\"one\" />\n" +
            "         <remote-server host=\"two\" />\n" +
            "         <connection-pool max-active=\"10\" exhausted-action=\"CREATE_NEW\" />\n" +
            "         <async-executor>\n" +
            "             <property name=\"maxThreads\">4</property>" +
            "         </async-executor>\n" +
            "         <write-behind/>\n" +
            "       </remote-store>\n" +
            "     </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );

      RemoteStoreConfiguration store = (RemoteStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assertEquals(2, store.servers().size());
      assertEquals(ExhaustedAction.CREATE_NEW, store.connectionPool().exhaustedAction());
      assertEquals(4, store.asyncExecutorFactory().properties().getIntProperty("maxThreads", 0));
      assertTrue(store.async().enabled());
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is, true, false);
      assert cacheManager.getDefaultCacheConfiguration().persistence().stores().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}
