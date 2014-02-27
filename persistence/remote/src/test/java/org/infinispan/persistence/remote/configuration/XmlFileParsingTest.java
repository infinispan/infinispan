package org.infinispan.persistence.remote.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

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
      String config = INFINISPAN_START_TAG +
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "     <persistence>\n" +
            "       <remote-store xmlns=\"urn:infinispan:config:store:remote:7.0\" >\n" +
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
            "</cache-container>" +
            TestingUtil.INFINISPAN_END_TAG;

      RemoteStoreConfiguration store = (RemoteStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert store.servers().size() == 2;
      assert store.connectionPool().exhaustedAction() == ExhaustedAction.CREATE_NEW;
      assert store.asyncExecutorFactory().properties().getIntProperty("maxThreads", 0) == 4;
      assert store.async().enabled();
   }

   private StoreConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().persistence().stores().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().persistence().stores().get(0);
   }
}