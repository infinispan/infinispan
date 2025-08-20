package org.infinispan.query.distributed;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.File;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.distributed.IndexWithSharedStoreTest")
public class IndexWithSharedStoreTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() {
      createCacheManager("1");
      createCacheManager("2");
   }

   private void createCacheManager(String globalStateDirectory) {
      var persistentLocation = tmpDirectory(getClass().getSimpleName(), globalStateDirectory);
      //noinspection ResultOfMethodCallIgnored
      new File(persistentLocation).mkdirs();
      var global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(persistentLocation).configurationStorage(ConfigurationStorage.OVERLAY);
      global.serialization().addContextInitializer(QueryTestSCI.INSTANCE);

      addClusterEnabledCacheManager(global, null);
   }

   @AfterClass(alwaysRun = true)
   protected void destroy() {
      try {
         super.destroy();
      } finally {
         Util.recursiveFileRemove(tmpDirectory(getClass().getSimpleName()));
      }
   }

   private static ConfigurationBuilder cacheConfiguration() {
      var builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.indexing().enable()
            .addIndexedEntities(Car.class)
            .storage(IndexStorage.FILESYSTEM)
            .startupMode(IndexStartupMode.AUTO);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(true)
            .storeName(IndexWithSharedStoreTest.class.getSimpleName());
      builder.memory().maxCount(1);
      return builder;
   }

   private static Car createCar(String carMake) {
      return new Car(carMake, "black", 1000);
   }

   private static String query(String carMake) {
      return String.format("FROM %s where make:'%s'", Car.class.getName(), carMake);
   }

   private static void queryAndAssert(Cache<String, Car> cache, Car expectedCar) {
      var cars = cache.<Car>query(query(expectedCar.getMake())).execute().list();
      AssertJUnit.assertNotNull(cars);
      AssertJUnit.assertEquals(1, cars.size());
      AssertJUnit.assertEquals(expectedCar.getMake(), cars.get(0).getMake());
   }

   public void testIndexesAfterRestart() {
      var cacheName = "test-indexes-after-restart";
      manager(0).defineConfiguration(cacheName, cacheConfiguration().build());
      manager(1).defineConfiguration(cacheName, cacheConfiguration().build());
      var cache0 = manager(0).<String, Car>getCache(cacheName);
      var cache1 = manager(1).<String, Car>getCache(cacheName);
      waitForClusterToForm(cacheName);

      var carA = createCar("A");
      cache0.put("car1", carA);

      queryAndAssert(cache0, carA);
      queryAndAssert(cache1, carA);

      killMember(1, cacheName, true);

      var carB = createCar("B");
      cache0.put("car1", carB);
      cache0.put("car2", createCar("C"));

      createCacheManager("2");
      manager(1).defineConfiguration(cacheName, cacheConfiguration().build());
      cache1 = manager(1).getCache(cacheName);
      waitForClusterToForm(cacheName);

      queryAndAssert(cache0, carB);
      queryAndAssert(cache1, carB);
   }
}
