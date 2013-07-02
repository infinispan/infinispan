package org.infinispan.lucene.profiling;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.LuceneKey2StringMapper;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Testcase verifying that the index is usable under stress even when a cachestore is configured.
 * See ISPN-575 (Corruption in data when using a permanent store)
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "profiling", testName = "lucene.profiling.CacheStoreStressTest", sequential = true)
public class CacheStoreStressTest extends SingleCacheManagerTest {
   
   private final ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();

   private static final String indexName = "tempIndexName";
   
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = CacheTestSupport.createLegacyTestConfiguration();
      enableTestJdbcStorage(configuration);
      return TestCacheManagerFactory.createClusteredCacheManager(configuration);
   }
   
   private void enableTestJdbcStorage(Configuration configuration) {
      TableManipulation tm = UnitTestDatabaseManager.buildStringTableManipulation();
      JdbcStringBasedCacheStoreConfig jdbcStoreConfiguration = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      jdbcStoreConfiguration.setKey2StringMapperClass(LuceneKey2StringMapper.class.getName());
      CacheLoaderManagerConfig loaderManagerConfig = configuration.getCacheLoaderManagerConfig();
      loaderManagerConfig.setPreload(Boolean.TRUE);
      loaderManagerConfig.addCacheLoaderConfig(jdbcStoreConfiguration);
   }

   @Test
   public void stressTestOnStore() throws InterruptedException, IOException {
      cache = cacheManager.getCache();
      assert cache!=null;
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
      PerformanceCompareStressTest.stressTestDirectory(dir, "InfinispanClusteredWith-Store");
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, indexName, true);
   }
   
}
