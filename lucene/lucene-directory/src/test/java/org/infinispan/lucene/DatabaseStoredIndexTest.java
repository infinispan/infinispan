package org.infinispan.lucene;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.removeByTerm;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import org.apache.lucene.store.Directory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.lucene.testutils.TestSegmentReadLocker;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Test to verify that it's possible to use the index using a JdbcStringBasedStore
 *
 * @see org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.DatabaseStoredIndexTest")
public class DatabaseStoredIndexTest extends SingleCacheManagerTest {

   private static final String DB_URL = "jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=0";

   /** The INDEX_NAME */
   private static final String INDEX_NAME = "testing index";

   private final HashMap cacheCopy = new HashMap();

   private Connection connection;

   public DatabaseStoredIndexTest() throws SQLException {
      cleanup = CleanupPhase.AFTER_METHOD;
      connection = DriverManager.getConnection(DB_URL, "sa", "");
   }

   @AfterClass(alwaysRun=true)
   public void closeDB() {
      try {
         connection.close();
      } catch (SQLException e) {
         AssertJUnit.fail("Could not close the keepalive DB connection");
      }
   }

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
            .simpleConnection()
               .driverClass(org.h2.Driver.class)
               .connectionUrl(DB_URL)
               .username("sa");
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   @Test
   public void testIndexUsage() throws IOException {
      cache = cacheManager.getCache();
      TestSegmentReadLocker testSegmentReadLocker = new TestSegmentReadLocker(cache, cache, cache, INDEX_NAME);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .overrideSegmentReadLocker(testSegmentReadLocker).create();
      writeTextToIndex(dir, 0, "hello database");
      assertTextIsFoundInIds(dir, "hello", 0);
      writeTextToIndex(dir, 1, "you have to store my index segments");
      writeTextToIndex(dir, 2, "so that I can shut down all nodes");
      writeTextToIndex(dir, 3, "and restart later keeping the index around");
      assertTextIsFoundInIds(dir, "index", 1, 3);
      removeByTerm(dir, "and");
      assertTextIsFoundInIds(dir, "index", 1);
      dir.close();
      for (Map.Entry me : cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).entrySet()) {
         cacheCopy.put(me.getKey(), me.getValue());
      }
      cache.stop();
      cacheManager.stop();
   }

   @Test(dependsOnMethods="testIndexUsage")
   public void indexWasStored() throws IOException {
      cache = cacheManager.getCache();
      boolean failed = false;
      for (Object key : cacheCopy.keySet()) {
         if (key instanceof FileReadLockKey) {
            log.error("Key found in store, shouldn't have persisted this or should have cleaned up all readlocks on directory close:" + key);
            failed = true;
         }
         else {
            Object expected = cacheCopy.get(key);
            Object actual = cache.get(key);
            if (expected==null && actual==null)
               continue;
            if (expected instanceof byte[]) {
               expected = Util.printArray((byte[]) expected, false);
               actual = Util.printArray((byte[]) actual, false);
            }
            if (expected == null || ! expected.equals(actual)) {
               log.error("Failure on key["+key.toString()+"] expected value:\n\t"+expected+"\tactual value:\n\t"+actual);
               failed = true;
            }
         }
      }
      AssertJUnit.assertFalse(failed);
      AssertJUnit.assertEquals("have a different number of keys", cacheCopy.keySet().size(), cache.keySet().size());
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();
      assertTextIsFoundInIds(dir, "index", 1);
      dir.close();
   }

}
