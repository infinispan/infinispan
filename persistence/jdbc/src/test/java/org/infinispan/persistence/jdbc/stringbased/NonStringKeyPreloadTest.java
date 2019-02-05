package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.clearCacheLoader;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-579.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.NonStringKeyPreloadTest")
public class NonStringKeyPreloadTest extends AbstractInfinispanTest {

   public void testPreloadWithKey2StringMapper() throws Exception {
      String mapperName = PersonKey2StringMapper.class.getName();
      ConfigurationBuilder cfg = createCacheStoreConfig(mapperName, true);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(cfg)) {
         @Override
         public void call() {
            try {
               cm.getCache();
               assert false : " Preload with Key2StringMapper is not supported. Specify an TwoWayKey2StringMapper if you want to support it (or disable preload).";
            } catch (CacheException e) {
               log.debugf("Ignoring expected exception", e);
            }
         }
      });
   }

   public void testPreloadWithTwoWayKey2StringMapper() throws Exception {
      String mapperName = TwoWayPersonKey2StringMapper.class.getName();
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true);
      final Person mircea = new Person("Markus", "Mircea", 30);
      final Person dan = new Person("Dan", "Dude", 30);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(config)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(mircea, "me");
            cache.put(dan, "mate");
         }
      });

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(config)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = null;
            try {
               cache = cm.getCache();
               assert cache.containsKey(mircea);
               assert cache.containsKey(dan);
            } finally {
               clearCacheLoader(cache);
            }
         }
      });
   }

   public void testPreloadWithTwoWayKey2StringMapperAndBoundedCache() throws Exception {
      String mapperName = TwoWayPersonKey2StringMapper.class.getName();
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true);
      config.memory().size(3);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(config)) {
         @Override
         public void call() {
            AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();
            for (int i = 0; i < 10; i++) {
               Person p = new Person("name" + i, "surname" + i, 30);
               cache.put(p, "" + i);
            }
         }
      });

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(config)) {
         @Override
         public void call() {
            AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();
            assertEquals(3, cache.getDataContainer().size());
            int found = 0;
            for (int i = 0; i < 10; i++) {
               Person p = new Person("name" + i, "surname" + i, 30);
               if (cache.getDataContainer().containsKey(p)) {
                  found++;
               }
            }
            assertEquals(3, found);
         }
      });
   }

   static ConfigurationBuilder createCacheStoreConfig(String mapperName, boolean preload) {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      JdbcStringBasedStoreConfigurationBuilder store = cfg
         .persistence()
            .connectionAttempts(1)
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
               .fetchPersistentState(true)
               .preload(preload)
               .key2StringMapper(mapperName);
      UnitTestDatabaseManager.buildTableManipulation(store.table());
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);

      return cfg;
   }
}
