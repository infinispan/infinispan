package org.infinispan.persistence.jdbc.stringbased;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTxFunctionalTest")
public class JdbcStringBasedStoreTxFunctionalTest extends JdbcStringBasedStoreFunctionalTest {
   private boolean transactionalConfig;
   private boolean sharedConfig;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new JdbcStringBasedStoreTxFunctionalTest().transactionalConfig(true).sharedConfig(false),
            new JdbcStringBasedStoreTxFunctionalTest().transactionalConfig(false).sharedConfig(false),
            new JdbcStringBasedStoreTxFunctionalTest().transactionalConfig(true).sharedConfig(true),
            new JdbcStringBasedStoreTxFunctionalTest().transactionalConfig(false).sharedConfig(true),
      };
   }

   @Override
   protected String parameters() {
      return " [transactionalConfig=" + transactionalConfig + ", sharedConfig=" + sharedConfig + "]";
   }

   private JdbcStringBasedStoreTxFunctionalTest transactionalConfig(boolean transactionalConfig) {
      this.transactionalConfig = transactionalConfig;
      return this;
   }

   private JdbcStringBasedStoreTxFunctionalTest sharedConfig(boolean sharedConfig) {
      this.sharedConfig = sharedConfig;
      return this;
   }

   @Override
   protected ConfigurationBuilder getDefaultCacheConfiguration() {
      ConfigurationBuilder configurationBuilder = super.getDefaultCacheConfiguration();
      configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      if (sharedConfig) {
         // Shared requires a clustered config, even though we have a single node
         configurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      }
      return configurationBuilder;
   }

   @Override
   protected void modifyJdbcConfiguration(JdbcStringBasedStoreConfigurationBuilder builder) {
      builder.transactional(transactionalConfig);
      builder.shared(sharedConfig);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(boolean start, GlobalConfigurationBuilder global,
         ConfigurationBuilder cb) {
      // Make sure defaults are transactional as well for created configs
      if (transactionalConfig) {
         cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (sharedConfig) {
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(Thread.currentThread().getContextClassLoader(), global);
         if (cb != null) {
            String defaultName = JdbcStringBasedStoreTxFunctionalTest.class.getName() + "-default";
            global.defaultCacheName(defaultName);
            holder.newConfigurationBuilder(defaultName).read(cb.build());
         }
         global.transport().defaultTransport();
         return TestCacheManagerFactory.createClusteredCacheManager(start, holder);
      } else {
         return super.createCacheManager(start, global, cb);
      }
   }

   @Override
   public void testRemoveCacheWithPassivation() {
      // Shared and purgeOnStartup don't mix
      // Transactional and passivation don't mix
      if (!sharedConfig && !transactionalConfig) {
         super.testRemoveCacheWithPassivation();
      }
   }

   @Override
   public void testStoreByteArrays(Method m) throws PersistenceException {
      // Shared and purgeOnStartup don't mix
      // Transactional and passivation don't mix
      if (!sharedConfig && !transactionalConfig) {
         super.testStoreByteArrays(m);
      }
   }

   public void testWithPassivation(Method m) throws Exception {
      ConfigurationBuilder base = getDefaultCacheConfiguration();
      base.persistence().passivation(true);

      ExceptionRunnable runnable = () -> TestingUtil.defineConfiguration(cacheManager, m.getName(), configureCacheLoader(base, m.getName(), false).build());
      // transactional and shared don't mix with passivation
      if (transactionalConfig || sharedConfig) {
         Exceptions.expectException(CacheConfigurationException.class, runnable);
      } else {
         runnable.run();
      }
   }

   public void testWithPurgeOnStartup(Method m) throws Exception {
      ConfigurationBuilder base = getDefaultCacheConfiguration();

      ExceptionRunnable runnable = () -> TestingUtil.defineConfiguration(cacheManager, m.getName(), configureCacheLoader(base, m.getName(), true).build());
      // shared doesn't mix with purgeOnStartup
      if (sharedConfig) {
         Exceptions.expectException(CacheConfigurationException.class, runnable);
      } else {
         runnable.run();
      }
   }

   public void testRollback() throws SystemException, NotSupportedException {
      String cacheName = "testRollback";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      String key = "rollback-test";
      assertNull(cache.get(key));

      TransactionManager manager = cache.getAdvancedCache().getTransactionManager();

      String value = "the-value";
      manager.begin();
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      manager.rollback();

      assertNull(cache.get(key));
   }
}
