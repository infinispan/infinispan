package org.infinispan.loaders.cassandra.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.configuration.XmlFileParsingTest")
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
            "       <cassandraStore xmlns=\"urn:infinispan:config:cassandra:6.0\" autoCreateKeyspace=\"false\" fetchPersistentState=\"true\" readConsistencyLevel=\"EACH_QUORUM\" writeConsistencyLevel=\"ANY\">\n" +
            "         <servers>\n" +
            "           <server host=\"one\" />\n" +
            "           <server host=\"two\" />\n" +
            "         </servers>\n" +
            "         <async enabled=\"true\" />\n" +
            "       </cassandraStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      CassandraCacheStoreConfiguration store = (CassandraCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert !store.autoCreateKeyspace();
      assert store.framed();
      assert store.servers().size() == 2;
      assert store.readConsistencyLevel().equals(ConsistencyLevel.EACH_QUORUM);
      assert store.writeConsistencyLevel().equals(ConsistencyLevel.ANY);
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