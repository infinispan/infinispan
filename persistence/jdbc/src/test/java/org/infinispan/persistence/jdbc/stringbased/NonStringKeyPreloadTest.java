package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.clearCacheLoader;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import java.util.function.Consumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-579.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.NonStringKeyPreloadTest")
public class NonStringKeyPreloadTest extends AbstractInfinispanTest {

   public void testPreloadWithKey2StringMapper() {
      String mapperName = PersonKey2StringMapper.class.getName();
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true);

      withCacheManager(() -> TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE), cm -> {
         try {
            cm.createCache("invalidCache", config.build());
            assert false : " Preload with Key2StringMapper is not supported. Specify an TwoWayKey2StringMapper if you want to support it (or disable preload).";
         } catch (CacheException e) {
            log.debugf("Ignoring expected exception", e);
         }
      });
   }

   public void testPreloadWithTwoWayKey2StringMapper() {
      String mapperName = TwoWayPersonKey2StringMapper.class.getName();
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true);
      final Person mircea = new Person("Mircea");
      final Person dan = new Person("Dan");

      withCacheManagerConfig(config, cm -> {
         Cache<Object, Object> cache = cm.getCache();
         cache.put(mircea, "me");
         cache.put(dan, "mate");
      });

      withCacheManagerConfig(config, cm -> {
         Cache<Object, Object> cache = null;
         try {
            cache = cm.getCache();
            assert cache.containsKey(mircea);
            assert cache.containsKey(dan);
         } finally {
            clearCacheLoader(cache);
         }
      });
   }

   public void testPreloadWithTwoWayKey2StringMapperAndBoundedCache() {
      String mapperName = TwoWayPersonKey2StringMapper.class.getName();
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true);
      config.memory().maxCount(3);

      withCacheManagerConfig(config, cm -> {
         for (int i = 0; i < 10; i++)
            cm.getCache().getAdvancedCache().put(new Person("name" + i), "" + i);
      });

      withCacheManagerConfig(config, cm -> {
         AdvancedCache<Object, Object> cache = cm.getCache().getAdvancedCache();
         assertEquals(3, cache.getDataContainer().size());
         int found = 0;
         for (int i = 0; i < 10; i++) {
            Person p = new Person("name" + i);
            if (cache.getDataContainer().containsKey(p)) {
               found++;
            }
         }
         assertEquals(3, found);
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

   private void withCacheManagerConfig(ConfigurationBuilder config, Consumer<EmbeddedCacheManager> consumer) {
      withCacheManager(() -> TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, config), consumer);
   }
}
