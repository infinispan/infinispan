package org.infinispan.lucene.profiling;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.LuceneKey2StringMapper;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testcase verifying that the index is usable under stress even when a cachestore is configured.
 * See ISPN-575 (Corruption in data when using a permanent store)
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "profiling", testName = "lucene.profiling.StoreStressTest", singleThreaded = true)
public class StoreStressTest extends SingleCacheManagerTest {

   private static final String indexName = "tempIndexName";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .preload(true)
            .key2StringMapper(LuceneKey2StringMapper.class)
            .table()
            .idColumnName("ID_COLUMN")
            .idColumnType("VARCHAR(255)")
            .tableNamePrefix("ISPN_JDBC")
            .dataColumnName("DATA_COLUMN")
            .dataColumnType("BLOB")
            .timestampColumnName("TIMESTAMP_COLUMN")
            .timestampColumnType("BIGINT")
            .connectionPool()
            .driverClass(org.h2.Driver.class)
            .connectionUrl("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=0")
            .username("sa");
      return TestCacheManagerFactory.createClusteredCacheManager(cb);
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
