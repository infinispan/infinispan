package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.clearCacheLoader;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import java.sql.Connection;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.jdbc.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.spi.PersistenceException;
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
      ConfigurationBuilder cfg = createCacheStoreConfig(mapperName, false, true);

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
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true, true);
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
      ConfigurationBuilder config = createCacheStoreConfig(mapperName, true, true);
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

   static ConfigurationBuilder createCacheStoreConfig(String mapperName, boolean wrap, boolean preload) {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      JdbcStringBasedStoreConfigurationBuilder store = cfg
         .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
               .fetchPersistentState(true)
               .preload(preload)
               .key2StringMapper(mapperName);
      UnitTestDatabaseManager.buildTableManipulation(store.table());
      if (wrap) {
         ConnectionFactoryConfigurationBuilder<?> tmp = UnitTestDatabaseManager.configureUniqueConnectionFactory(new ConfigurationBuilder().persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class));
         store.connectionFactory(new SharedConnectionFactoryConfigurationBuilder(store)).read((PooledConnectionFactoryConfiguration)tmp.create());
      } else {
         UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      }

      return cfg;
   }

   public static class SharedConnectionFactory extends ConnectionFactory {
      static PooledConnectionFactory sharedFactory;
      static boolean started = false;

      @Override
      public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws PersistenceException {
         if (!started) {
            sharedFactory = new PooledConnectionFactory();
            sharedFactory.start(config, classLoader);
            started = true;
         }
      }

      @Override
      public void stop() {
         //ignores
      }

      @Override
      public Connection getConnection() throws PersistenceException {
         return sharedFactory.getConnection();
      }

      @Override
      public void releaseConnection(Connection conn) {
         sharedFactory.releaseConnection(conn);
      }
   }

   @BuiltBy(SharedConnectionFactoryConfigurationBuilder.class)
   public static class SharedConnectionFactoryConfiguration extends PooledConnectionFactoryConfiguration {

      protected SharedConnectionFactoryConfiguration(AttributeSet attributes) {
         super(attributes);
      }

      @Override
      public Class<? extends ConnectionFactory> connectionFactoryClass() {
         return SharedConnectionFactory.class;
      }
   }

   public static class SharedConnectionFactoryConfigurationBuilder extends PooledConnectionFactoryConfigurationBuilder {
      public SharedConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder builder) {
         super(builder);
      }
      @Override
      public void validate() {
      }

      @Override
      public void validate(GlobalConfiguration globalConfig) {
      }
   }
}
