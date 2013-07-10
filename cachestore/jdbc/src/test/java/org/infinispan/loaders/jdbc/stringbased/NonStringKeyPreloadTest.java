package org.infinispan.loaders.jdbc.stringbased;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.test.TestingUtil.clearCacheLoader;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.sql.Connection;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.configuration.AbstractJdbcCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.configuration.AbstractJdbcCacheStoreConfigurationChildBuilder;
import org.infinispan.loaders.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.loaders.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
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
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.NonStringKeyPreloadTest")
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
               // Expected
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
      config.eviction().strategy(EvictionStrategy.LRU).maxEntries(3);
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

      JdbcStringBasedCacheStoreConfigurationBuilder store = cfg
         .loaders()
            .preload(preload)
            .addStore(JdbcStringBasedCacheStoreConfigurationBuilder.class)
               .fetchPersistentState(true)
               .key2StringMapper(mapperName);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      if (wrap) {
         ConnectionFactoryConfigurationBuilder<?> tmp = UnitTestDatabaseManager.configureUniqueConnectionFactory(new ConfigurationBuilder().loaders().addStore(JdbcStringBasedCacheStoreConfigurationBuilder.class));
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
      public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws CacheLoaderException {
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
      public Connection getConnection() throws CacheLoaderException {
         return sharedFactory.getConnection();
      }

      @Override
      public void releaseConnection(Connection conn) {
         sharedFactory.releaseConnection(conn);
      }
   }

   @BuiltBy(SharedConnectionFactoryConfigurationBuilder.class)
   public static class SharedConnectionFactoryConfiguration extends PooledConnectionFactoryConfiguration {
      SharedConnectionFactoryConfiguration(String connectionUrl, String driverClass, String username, String password) {
         super(connectionUrl, driverClass, username, password);
      }

      @Override
      public Class<? extends ConnectionFactory> connectionFactoryClass() {
         return SharedConnectionFactory.class;
      }
   }

   public static class SharedConnectionFactoryConfigurationBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>> extends AbstractJdbcCacheStoreConfigurationChildBuilder<S> implements ConnectionFactoryConfigurationBuilder<SharedConnectionFactoryConfiguration> {

      public SharedConnectionFactoryConfigurationBuilder(AbstractJdbcCacheStoreConfigurationBuilder<?, S> builder) {
         super(builder);
      }

      private String connectionUrl;
      private String driverClass;
      private String username;
      private String password;

      @Override
      public void validate() {
      }

      @Override
      public SharedConnectionFactoryConfiguration create() {
         return new SharedConnectionFactoryConfiguration(connectionUrl, driverClass, username, password);
      }

      @Override
      public Builder<?> read(SharedConnectionFactoryConfiguration template) {
         this.connectionUrl = template.connectionUrl();
         this.driverClass = template.driverClass();
         this.username = template.username();
         this.password = template.password();
         return this;
      }

      public Builder<?> read(PooledConnectionFactoryConfiguration template) {
         this.connectionUrl = template.connectionUrl();
         this.driverClass = template.driverClass();
         this.username = template.username();
         this.password = template.password();
         return this;
      }

   }
}
