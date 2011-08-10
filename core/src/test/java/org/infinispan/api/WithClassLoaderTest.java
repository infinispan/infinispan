package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CherryPickClassLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
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
   private ClassLoader systemCl;

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cm0 = createCacheManager(GlobalConfiguration.getClusteredDefault());
      cm0.getDefaultConfiguration().fluent()
         .clustering().mode(CacheMode.REPL_SYNC)
         .storeAsBinary().build();
      cacheManagers.add(cm0);

      String[] notFound = new String[]{CAR};
      systemCl = Thread.currentThread().getContextClassLoader();
      CherryPickClassLoader cl = new CherryPickClassLoader(null, null, notFound, systemCl);
      EmbeddedCacheManager cm1 = createCacheManager(GlobalConfiguration.getClusteredDefault(cl));
      cm1.getDefaultConfiguration().fluent()
         .clustering().mode(CacheMode.REPL_SYNC)
         .storeAsBinary().build();
      cacheManagers.add(cm1);
   }

   public void testReadingWithCorrectClassLoader() {
      Cache<Integer, Car> cache0 = cache(0);
      Car value = new Car().plateNumber("1234");
      cache0.put(1, value);

      Cache<Integer, Car> cache1 = cache(1);

      try {
         cache1.get(1);
         fail("Expected a class ClassNotFoundException");
      } catch (CacheException e) {
         if (!(e.getCause() instanceof ClassNotFoundException))
            throw e;
      }

      assertEquals(value, cache1.getAdvancedCache().with(systemCl).get(1));
   }

   public static class Car implements Serializable {
      String plateNumber;
      Car plateNumber(String s) { plateNumber = s; return this; }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Car car = (Car) o;

         if (plateNumber != null ? !plateNumber.equals(car.plateNumber) : car.plateNumber != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return plateNumber != null ? plateNumber.hashCode() : 0;
      }
   }

}
