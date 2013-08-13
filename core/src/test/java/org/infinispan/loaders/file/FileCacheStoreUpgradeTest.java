package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Address;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests upgrading file cache store from the old format to the new format.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "loaders.file.FileCacheStoreUpgradeTest")
public class FileCacheStoreUpgradeTest extends AbstractInfinispanTest {

   private String tmpDirectory;
   private String backupDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
      backupDirectory = TestingUtil.tmpDirectory(this, ".backup");
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
      TestingUtil.recursiveFileRemove(backupDirectory);
   }

   public void testUpgradeFileCacheStore() throws Exception {
      // 1. Write with old cache store and verify contents
      final Address address = new Address().setStreet("Sevogelstrasse").setCity("Basel").setZip(4001);
      final PojoWithSerializeWith pojo = new PojoWithSerializeWith(99, "any-key");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable(); // Force having a marshaller
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<?, ?> cache = cm.getCache();
            try {
               BucketFileCacheStore fs = createBucketFileCacheStore(cache);
               fs.store(TestInternalCacheEntryFactory.create("1", "v1"));
               fs.store(TestInternalCacheEntryFactory.create("2", address));
               fs.store(TestInternalCacheEntryFactory.create("3", pojo));
               assertEquals("v1", fs.load("1").getValue());
               assertEquals(address, fs.load("2").getValue());
               assertEquals(pojo, fs.load("3").getValue());
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      });

      // 2. Start with new cache store, perform upgrade and verify data
      builder = new ConfigurationBuilder();
      builder.loaders().addFileCacheStore().location(tmpDirectory);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<String, Object> cache = cm.getCache();
            CacheStore delegate = extractComponent(cache, CacheLoaderManager.class).getCacheStore();
            DelegateFileCacheStore store = (DelegateFileCacheStore) delegate;
            assertTrue(store.getCacheStoreDelegate() instanceof FileCacheStore);
            assertEquals("v1", cache.get("1"));
            assertEquals(address, cache.get("2"));
            assertEquals(pojo, cache.get("3"));
         }
      });

      // Clear tmp directory to make sure data it's read from the backup
      TestingUtil.recursiveFileRemove(tmpDirectory);

      // 3. Verify that the backup is correct, in case it's desirable to access it
      builder = new ConfigurationBuilder();
      builder.loaders().addFileCacheStore()
            .location(tmpDirectory).deprecatedBucketFormat(true);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<String, Object> cache = cm.getCache();
            CacheStore delegate = extractComponent(cache, CacheLoaderManager.class).getCacheStore();
            DelegateFileCacheStore store = (DelegateFileCacheStore) delegate;
            assertTrue("Store is: " + store.getCacheStoreDelegate(),
                  store.getCacheStoreDelegate() instanceof BucketFileCacheStore);
            assertEquals("v1", cache.get("1"));
            assertEquals(address, cache.get("2"));
            assertEquals(pojo, cache.get("3"));
         }
      });
   }

   private BucketFileCacheStore createBucketFileCacheStore(Cache<?, ?> cache) throws Exception {
      BucketFileCacheStore fcs = new BucketFileCacheStore();
      FileCacheStoreConfigurationBuilder fileBuilder =
            TestCacheManagerFactory.getDefaultCacheConfiguration(false)
                  .loaders().addFileCacheStore();

      fileBuilder.location(this.tmpDirectory)
            .purgeSynchronously(true);
      fcs.init(fileBuilder.create(), cache, extractCacheMarshaller(cache));
      fcs.start();
      return fcs;
   }

}
