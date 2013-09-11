package org.infinispan.persistence.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.binary.BinaryStoreWithManagedConnectionTest")
public class BinaryStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcBinaryStoreConfigurationBuilder storeBuilder = cc
            .persistence()
               .addStore(JdbcBinaryStoreConfigurationBuilder.class);
      storeBuilder.dataSource().jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);

      cacheManager = TestCacheManagerFactory.createCacheManager(cc);

      cache = cacheManager.getCache();
      JdbcBinaryStore jdbcBinaryCacheStore = (JdbcBinaryStore) TestingUtil.getFirstWriter(cache);
      assert jdbcBinaryCacheStore.getConnectionFactory() instanceof ManagedConnectionFactory;
      csc = jdbcBinaryCacheStore.getConfiguration();
      return jdbcBinaryCacheStore;
   }

   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      super.tearDown();
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      StreamingMarshaller component = cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
      assert component != null;
      return component;
   }

   public void testLoadFromFile() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/managed/binary-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
         assert firstCacheLoaderConfig != null;
         StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcBinaryStoreConfiguration;
         assert secondCacheLoaderConfig instanceof JdbcBinaryStoreConfiguration;
         JdbcBinaryStore loader = (JdbcBinaryStore) TestingUtil.getFirstLoader(first);
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         try {
            TestingUtil.killCacheManagers(cm);
         } catch (Throwable e) {
            e.printStackTrace();
         }
      }
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/BinaryStoreWithManagedConnectionTest/DS";
   }

   @Override
   public void testLoadAll() throws CacheLoaderException {
      super.testLoadAll();    // TODO: Customise this generated block
   }

   @Override
   public void testLoadAndStoreImmortal() throws CacheLoaderException {
      super.testLoadAndStoreImmortal();    // TODO: Customise this generated block
   }

   @Override
   public void testLoadAndStoreWithIdle() throws Exception {
      super.testLoadAndStoreWithIdle();    // TODO: Customise this generated block
   }

   @Override
   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      super.testLoadAndStoreWithLifespanAndIdle();    // TODO: Customise this generated block
   }

   @Override
   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
      super.testStopStartDoesNotNukeValues();    // TODO: Customise this generated block
   }
}
