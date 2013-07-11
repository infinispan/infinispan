package org.infinispan.loaders.cloud.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cloud.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <cloudStore xmlns=\"urn:infinispan:config:cloud:6.0\" cloudService=\"transient\" identity=\"me\" password=\"s3cr3t\" secure=\"true\" proxyHost=\"my-proxy\" proxyPort=\"8080\" fetchPersistentState=\"true\">\n" +
            "         <async enabled=\"true\" />\n" +
            "       </cloudStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      CloudCacheStoreConfiguration store = (CloudCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert store.cloudService().equals("transient");
      assert store.identity().equals("me");
      assert store.password().equals("s3cr3t");
      assert store.proxyHost().equals("my-proxy");
      assert store.proxyPort() == 8080;
      assert store.secure();
      assert store.fetchPersistentState();
      assert store.async().enabled();
   }

   private CacheLoaderConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().get(0);
   }
}