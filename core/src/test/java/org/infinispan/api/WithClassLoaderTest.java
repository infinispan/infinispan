package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CherryPickClassLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.infinispan.test.fwk.TestCacheManagerFactory.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * A test that verifies the correctness of {@link org.infinispan.AdvancedCache#with(ClassLoader)} API.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "api.WithClassLoaderTest")
public class WithClassLoaderTest extends MultipleCacheManagersTest {

   private static final String BASE = WithClassLoaderTest.class.getName() + "$";
   private static final String CAR = BASE + "Car";
   protected ClassLoader systemCl;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable()
            .clustering()
               .cacheMode(org.infinispan.configuration.cache.CacheMode.REPL_SYNC);
      EmbeddedCacheManager cm0 = createClusteredCacheManager(builder);
      cacheManagers.add(cm0);

      String[] notFound = new String[]{CAR};
      systemCl = Thread.currentThread().getContextClassLoader();
      CherryPickClassLoader cl = new CherryPickClassLoader(null, null, notFound, systemCl);

      GlobalConfigurationBuilder gcBuilder = createSecondGlobalCfgBuilder(cl);
      EmbeddedCacheManager cm1 = createClusteredCacheManager(gcBuilder, builder);
      cacheManagers.add(cm1);
   }

   protected GlobalConfigurationBuilder createSecondGlobalCfgBuilder(ClassLoader cl) {
      GlobalConfigurationBuilder gcBuilder =
            GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcBuilder.classLoader(cl);
      return gcBuilder;
   }

   public void testReadingWithCorrectClassLoaderAfterReplication() {
      writeReadWithCorrectClassLoader(this.<Integer, Car>cache(1).getAdvancedCache());
   }

   public void testReadingWithCorrectClassLoaderAfterReplicationWithDelegateCache() {
      AdvancedCache<Integer, Car> cache = advancedCache(1);
      AdvancedCache<Integer, Car> delegate =
            new CustomDelegateCache<Integer, Car>(cache);
      writeReadWithCorrectClassLoader(delegate);
   }

   private void writeReadWithCorrectClassLoader(AdvancedCache<Integer, Car> readWithCache) {
      AdvancedCache<Integer, Car> c0 = advancedCache(0);
      AdvancedCache<Integer, Car> c1 = advancedCache(1);
      Car value = new Car().plateNumber("1234");
      c0.put(1, value);

      try {
         c1.get(1);
         fail("Expected a ClassNotFoundException");
      } catch (CacheException e) {
         if (!(e.getCause() instanceof ClassNotFoundException))
            throw e;
      }

      assertEquals(value, readWithCache.with(systemCl).get(1));
   }

   // TODO: Add test where contents come from state transfer rather than replication
   // TODO: For that to work, memory state might need wrapping in a cache rpc command (i.e. state transfer command)

   public static class Car implements Serializable {
      String plateNumber;
      public Car plateNumber(String s) { plateNumber = s; return this; }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Car car = (Car) o;

         return !(plateNumber != null
                        ? !plateNumber.equals(car.plateNumber)
                        : car.plateNumber != null);
      }

      @Override
      public int hashCode() {
         return plateNumber != null ? plateNumber.hashCode() : 0;
      }
   }

   public static class CustomDelegateCache<K, V>
         extends AbstractDelegatingAdvancedCache<K, V> {

      public CustomDelegateCache(AdvancedCache<K, V> cache) {
         super(cache, new AdvancedCacheWrapper<K, V>() {
            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
               return new CustomDelegateCache<K, V>(cache);
            }
         });
      }
   }

}
