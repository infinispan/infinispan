package org.infinispan.it.osgi.persistence.leveldb;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class JniLevelDBStoreFunctionalTest extends BaseStoreFunctionalTest {

   private static String tmpDirectory;

   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @BeforeClass
   public static void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(JniLevelDBStoreFunctionalTest.class);
   }

   @AfterClass
   public static void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   @Before
   @Override
   public void setup() throws Exception {
      TestResourceTracker.backgroundTestStarted(this);
      super.setup();
   }

   @After
   @Override
   public void teardown() {
      super.teardown();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder p, boolean preload) {
      createStoreBuilder(p)
            .preload(preload)
            .implementationType(LevelDBStoreConfiguration.ImplementationType.JNI);
      return p;
   }

   @Test
   public void testTwoCachesSameCacheStore() {
      super.testTwoCachesSameCacheStore();
   }

   @Test
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();
   }

   @Test
   public void testPreloadStoredAsBinary() {
      super.testPreloadStoredAsBinary();
   }

   @Test
   public void testRestoreAtomicMap() throws Exception {
      super.testRestoreAtomicMap(this.getClass().getMethod("testRestoreAtomicMap"));
   }

   @Test
   public void testRestoreTransactionalAtomicMap() throws Exception {
      super.testRestoreTransactionalAtomicMap(this.getClass().getMethod("testRestoreTransactionalAtomicMap"));
   }

   @Test
   public void testStoreByteArrays() throws Exception {
      super.testStoreByteArrays(this.getClass().getMethod("testStoreByteArrays"));
   }

   LevelDBStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      return loaders.addStore(LevelDBStoreConfigurationBuilder.class).location(tmpDirectory + "/data").expiredLocation(tmpDirectory + "/expiry").clearThreshold(2);
   }

}
