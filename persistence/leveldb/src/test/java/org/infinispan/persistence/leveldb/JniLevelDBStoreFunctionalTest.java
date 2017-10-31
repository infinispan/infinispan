package org.infinispan.persistence.leveldb;

import java.lang.reflect.Method;

import org.infinispan.commons.test.skip.OS;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"unit", "smoke"}, testName = "persistence.leveldb.JniLevelDBStoreFunctionalTest")
public class JniLevelDBStoreFunctionalTest extends LevelDBStoreFunctionalTest {

   @BeforeClass
   public void skipOnOS() {
      SkipTestNG.skipOnOS(OS.SOLARIS, OS.WINDOWS);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder p, boolean preload) {
      super.createStoreBuilder(p)
            .preload(preload)
            .implementationType(LevelDBStoreConfiguration.ImplementationType.JNI);
      return p;
   }

   @Override
   public void testTwoCachesSameCacheStore() {
      super.testTwoCachesSameCacheStore();
   }

   @Override
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();
   }

   @Override
   public void testPreloadStoredAsBinary() {
      super.testPreloadStoredAsBinary();
   }

   @Override
   public void testRestoreAtomicMap(Method m) {
      super.testRestoreAtomicMap(m);
   }

   @Override
   public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
      super.testRestoreTransactionalAtomicMap(m);
   }

   @Override
   public void testStoreByteArrays(Method m) throws PersistenceException {
      super.testStoreByteArrays(m);
   }

   @Override
   public void testRemoveCache() {
      super.testRemoveCache();
   }

   @Override
   public void testRemoveCacheWithPassivation() {
      super.testRemoveCacheWithPassivation();
   }
}
