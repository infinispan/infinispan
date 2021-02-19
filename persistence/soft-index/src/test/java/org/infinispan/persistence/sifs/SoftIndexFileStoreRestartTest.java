package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Reproducer for ISPN-12630
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreRestartTest")
public class SoftIndexFileStoreRestartTest extends SingleCacheManagerTest {

   private static final String KEY = "key";

   private String tmpDirectory;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL);
      builder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .segmented(false)
            .indexLocation(tmpDirectory)
            .dataLocation(tmpDirectory)
            .purgeOnStartup(false); //avoid cleanup the store after restarting
      GlobalConfigurationBuilder gBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      gBuilder.defaultCacheName(getDefaultCacheName());
      return TestCacheManagerFactory.createCacheManager(gBuilder, builder);
   }

   @BeforeClass(alwaysRun = true)
   @Override
   protected void createBeforeClass() throws Exception {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      Util.recursiveFileRemove(tmpDirectory);
   }

   public void testNoDuplicatesRestart() throws Exception {
      assertTrue(cache.entrySet().isEmpty());

      cache.put(KEY, "v1");
      assertEntrySet("v1");

      restart();

      assertEntrySet("v1");
      cache.put(KEY, "v2");
      assertEntrySet("v2");

      restart();

      assertEntrySet("v2");
      cache.put(KEY, "v3");
      assertEntrySet("v3");

      restart();
      assertEntrySet("v3");
   }

   private void assertEntrySet(String value) {
      Collection<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(1, entrySet.size());
      Map.Entry<Object, Object> entry = entrySet.iterator().next();
      assertEquals(KEY, entry.getKey());
      assertEquals(value, entry.getValue());
      assertEquals(value, cache.get(KEY));
   }

   private void restart() throws Exception {
      stop();
      setup();
   }

   private void stop() {
      TestingUtil.killCacheManagers(cacheManager);
      cache = null;
      cacheManager = null;
   }
}
