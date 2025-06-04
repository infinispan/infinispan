package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.persistence.jdbc.UnitTestDatabaseManager.buildTableManipulation;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


@Test(groups = "functional", testName = "persistence.jdbc.stringbased.AbstractStringBasedCacheStore")
public abstract class AbstractStringBasedCacheStore extends AbstractInfinispanTest {

    protected EmbeddedCacheManager dcm;
    protected ConnectionFactory connectionFactory;
    protected TableManager tableManager;
    protected TableManipulationConfiguration tableConfiguration;
    protected Cache<String, String> cache;

   @AfterMethod
   public void cleanAfterEach() {
      tableManager.stop();
      connectionFactory.stop();
      TestingUtil.killCacheManagers(dcm);
   }

   @Test
    public void testPutGetRemoveWithoutPassivationWithPreload() throws Exception {
            dcm = configureCacheManager(false, true, false);
            cache = dcm.getCache();
            assertCleanCacheAndStore(cache);

            cache.put("k1", "v1");
            cache.put("k2", "v2");

            // test passivation==false, database should contain all entries which are in the cache
            assertNotNull(getValueByKey("k1"));
            assertNotNull(getValueByKey("k2"));

            cache.stop();
            cache.start();

            assertNotNull(getValueByKey("k1"));
            assertNotNull(getValueByKey("k2"));
            assertEquals("v1", cache.get("k1"));
            assertEquals("v2", cache.get("k2"));
            // when the entry is removed from the cache, it should be also removed from the cache store (the store
            // and the cache are the same sets of keys)
            cache.remove("k1");
            assertNull(cache.get("k1"));
            assertNull(getValueByKey("k1"));
    }

    @Test
    public void testPutGetRemoveWithPassivationWithoutPreload() throws Exception {
            dcm = configureCacheManager(true, false, true);
            cache = dcm.getCache();
            assertCleanCacheAndStore(cache);

            cache.put("k1", "v1");
            cache.put("k2", "v2");
            //not yet in store (eviction.max-entries=2)
            assertNull(getValueByKey("k1"));
            assertNull(getValueByKey("k2"));
            cache.put("k3", "v3");
            assertEquals("v3", cache.get("k3"));
            //now some key is evicted and stored in store
            assertEquals(2, cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());
            // Passivation is async
            eventuallyEquals(1, () -> getAllRows().size());
            //retrieve from store to cache and remove from store, another key must be evicted
            cache.get("k1");
            cache.get("k2");
            assertEquals(2, cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());
            eventuallyEquals(2,  () -> getAllRows().size());

            cache.stop();
            cache.start();

            assertEquals(0, cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());
            // test purge==false, entries should remain in the database after restart
            assertEquals(3, getAllRows().size());
            assertNotNull(getValueByKey("k1"));
            assertEquals("v1", cache.get("k1"));

            cache.remove("k1");
            assertNull(cache.get("k1"));
            assertNull(getValueByKey("k1"));
    }

    public EmbeddedCacheManager configureCacheManager(boolean passivation, boolean preload, boolean eviction) {
        GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName("StringBasedCache");
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
                .persistence()
                .passivation(passivation)
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .preload(preload);

        connectionFactory = getConnectionFactory(storeBuilder);
        setTableManipulation(storeBuilder);

        if (eviction) {
            builder.memory().maxCount(2);
        } else {
            builder.memory().maxCount(-1);
        }

        tableConfiguration = storeBuilder.create().table();
        EmbeddedCacheManager defaultCacheManager = TestCacheManagerFactory.newDefaultCacheManager(true, gcb, builder);
        String cacheName = defaultCacheManager.getCache().getName();
        PersistenceMarshaller marshaller = ComponentRegistry.of(defaultCacheManager.getCache()).getPersistenceMarshaller();
        InitializationContext ctx = new DummyInitializationContext(null, null, marshaller, null, null, null, null, null, null, null);
        tableManager = TableManagerFactory.getManager(ctx, connectionFactory, storeBuilder.create(), cacheName);

        return defaultCacheManager;
    }

    protected void setTableManipulation(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        buildTableManipulation(storeBuilder.table());
    }

    protected abstract ConnectionFactory getConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder);

    protected void assertCleanCacheAndStore(Cache<String, String> cache) throws Exception {
        cache.clear();
        deleteAllRows();
        assertEquals(0, cache.size());
        assertNull(getValueByKey("k1"));
        assertNull(getValueByKey("k2"));
    }

    public Object getValueByKey(String key) throws Exception {
        Object result = null;
        Connection connection = connectionFactory.getConnection();
        PreparedStatement ps = connection.prepareStatement(tableManager.getSelectRowSql());
        ps.setString(1, key);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            result = rs.getObject(tableConfiguration.idColumnName()); //start from 1, not 0
        }
        connectionFactory.releaseConnection(connection);

        return result;

    }

    private List<String> getAllRows() {
       try {
          Connection connection = connectionFactory.getConnection();
          Statement s = connection.createStatement();
          ResultSet rs = s.executeQuery(tableManager.getLoadAllRowsSql());
          List<String> rows = new ArrayList<>();
          while (rs.next()) {
             rows.add(rs.toString());
          }
          connectionFactory.releaseConnection(connection);
          return rows;
       } catch (Throwable t) {
          throw new AssertionError(t);
       }
    }

    private void deleteAllRows() throws Exception {
        Connection connection = connectionFactory.getConnection();
        Statement s = connection.createStatement();
        s.executeUpdate(tableManager.getDeleteAllSql());
        connectionFactory.releaseConnection(connection);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
       connectionFactory.stop();
    }
}
