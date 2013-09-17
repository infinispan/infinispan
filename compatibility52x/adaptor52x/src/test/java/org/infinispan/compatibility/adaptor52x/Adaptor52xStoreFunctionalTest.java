package org.infinispan.compatibility.adaptor52x;

import org.infinispan.compatibility.loaders.Custom52xCacheStore;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.test.TestingUtil;
import org.junit.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.adaptor52x.Adaptor52xStoreFunctionalTest")
public class Adaptor52xStoreFunctionalTest extends BaseStoreFunctionalTest {

   public static final String DIR =  "__Adaptor52xStoreFunctionalTest__";

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      Properties properties = new Properties();
      properties.put("location", DIR);
      loaders.addStore(Adaptor52xStoreConfigurationBuilder.class)
            .loader(new Custom52xCacheStore())
            .withProperties(properties)
            .preload(preload);
      return loaders;
   }

   @AfterClass
   public void removeDir() {
      TestingUtil.recursiveFileRemove(DIR);
   }

   @Override
   public void testStoreByteArrays(Method m) throws CacheLoaderException {
      //byte arrays are not supported in 5.2.x
   }

   @Override
   public void testTwoCachesSameCacheStore() {
   }
}
