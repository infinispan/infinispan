package org.infinispan.loaders.jdbc.binary;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * JdbcBinaryCacheStoreFunctionalTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.JdbcBinaryCacheStoreFunctionalTest")
public class JdbcBinaryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      JdbcBinaryStoreConfigurationBuilder store = loaders
         .addStore(JdbcBinaryStoreConfigurationBuilder.class).preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }

   @Override
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();    // TODO: Customise this generated block
   }

   @Override
   public void testPreloadStoredAsBinary() {
      super.testPreloadStoredAsBinary();    // TODO: Customise this generated block
   }

   @Override
   public void testStoreByteArrays(Method m) throws CacheLoaderException {
      super.testStoreByteArrays(m);    // TODO: Customise this generated block
   }
}
