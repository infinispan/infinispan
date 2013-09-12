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
            "   <default>\n" +
            "     <persistence>\n" +
            "       <remoteStore xmlns=\"urn:infinispan:config:remote:6.0\" >\n" +
            "         <servers>\n" +
            "           <server host=\"one\" />\n" +
            "           <server host=\"two\" />\n" +
            "         </servers>\n" +
            "         <connectionPool maxActive=\"10\" exhaustedAction=\"CREATE_NEW\" />\n" +
            "         <asyncTransportExecutor>\n" +
            "           <properties>\n" +
            "             <property name=\"maxThreads\" value=\"4\" />" +
            "           </properties>\n" +
            "         </asyncTransportExecutor>\n" +
            "         <async enabled=\"true\" />\n" +
            "       </remoteStore>\n" +
            "     </persistence>\n" +
            "   </default>\n" +
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