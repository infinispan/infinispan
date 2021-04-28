package org.infinispan.configuration.parsing;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test the migrator store to ensure it properly copies over the contents of the SFS to a SIFS for migration purposes.
 * @author William Burns
 * @since 13.0
 */
@Test(groups = "functional", testName = "configuration.parsing.SFSToSIFSMigratorTest")
public class SFSToSIFSMigratorTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "testCache";

   @AfterClass
   protected void clearTempDirectory() {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   private enum StoreType {
      SINGLE_NON_SEGMENTED {
         @Override
         void apply(ConfigurationBuilder configurationBuilder) {
            configurationBuilder.persistence().addSingleFileStore().segmented(false);
         }
      },
      SINGLE_SEGMENTED {
         @Override
         void apply(ConfigurationBuilder configurationBuilder) {
            configurationBuilder.persistence().addSingleFileStore().segmented(true);
         }
      },
      MIGRATING {
         @Override
         void apply(ConfigurationBuilder configurationBuilder) {
            configurationBuilder.persistence().addStore(SFSToSIFSConfigurationBuilder.class);
         }
      },
      SOFT_INDEX {
         @Override
         void apply(ConfigurationBuilder configurationBuilder) {
            configurationBuilder.persistence().addSoftIndexFileStore();
         }
      };

      abstract void apply(ConfigurationBuilder configurationBuilder);
   }

   @DataProvider(name = "singleFileStoreTypes")
   Object[][] singleTypes() {
      return new Object[][] {
            { StoreType.SINGLE_NON_SEGMENTED },
            { StoreType. SINGLE_SEGMENTED },
      };
   }

   /**
    * Test that makes sure data can be migrated from two different stores (in this case SingleFileStore and SoftIndex).
    * Note that SIFS only supports being segmented so we don't need a configuration to change that.
    */
   @Test(dataProvider = "singleFileStoreTypes")
   public void testStoreMigration(StoreType storeType) {
      GlobalConfigurationBuilder globalConfigurationBuilder = globalConfiguration();
      EmbeddedCacheManager embeddedCacheManager = createManager(storeType, globalConfigurationBuilder);

      try {
         Cache<String, String> sfsCache = embeddedCacheManager.getCache(CACHE_NAME);

         sfsCache.put("key", "value");

         TestingUtil.killCacheManagers(embeddedCacheManager);

         // Now create a manager which will migrate from SFS to SIFS
         embeddedCacheManager = createManager(StoreType.MIGRATING, globalConfigurationBuilder);

         Cache<String, String> sifsCache = embeddedCacheManager.getCache(CACHE_NAME);

         assertEquals("value", sifsCache.get("key"));

         TestingUtil.killCacheManagers(embeddedCacheManager);

         // Have a manger with only soft index and make sure it works properly
         embeddedCacheManager = createManager(StoreType.SOFT_INDEX, globalConfigurationBuilder);

         sifsCache = embeddedCacheManager.getCache(CACHE_NAME);

         assertEquals("value", sifsCache.get("key"));
      } finally {
         TestingUtil.killCacheManagers(embeddedCacheManager);
      }
   }

   private GlobalConfigurationBuilder globalConfiguration() {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName());
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      return global;
   }

   private EmbeddedCacheManager createManager(StoreType storeType, GlobalConfigurationBuilder global) {
      ConfigurationBuilder config = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      storeType.apply(config);
      EmbeddedCacheManager manager = createCacheManager(global, new ConfigurationBuilder());
      manager.defineConfiguration(CACHE_NAME, config.build());
      return manager;
   }
}
