package org.infinispan.it.osgi.persistence.jdbc.stringbased;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.it.osgi.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * Test similar to {@link org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStoreFunctionalTest}.
 *
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class JdbcStringBasedStoreFunctionalTest extends BaseStoreFunctionalTest {
   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = persistence
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      UnitTestDatabaseManager.setDialect(store);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return persistence;
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

}
