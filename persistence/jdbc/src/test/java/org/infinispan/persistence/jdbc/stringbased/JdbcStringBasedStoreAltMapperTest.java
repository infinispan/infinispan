package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tester for {@link JdbcStringBasedStore} with an alternative {@link org.infinispan.persistence.keymappers.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperTest")
public class JdbcStringBasedStoreAltMapperTest extends AbstractInfinispanTest {

   protected AdvancedLoadWriteStore cacheStore;
   protected TableManager tableManager;
   protected static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   protected static final Person MANIK = new Person("Manik", "Surtani", 18);
   protected StreamingMarshaller marshaller;

   protected JdbcStringBasedStoreConfigurationBuilder createJdbcConfig(ConfigurationBuilder builder) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .key2StringMapper(PersonKey2StringMapper.class);
      return storeBuilder;
   }

   @BeforeClass
   public void createCacheStore() throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = createJdbcConfig(builder);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      cacheStore = new JdbcStringBasedStore();
      marshaller = new TestObjectStreamMarshaller();
      cacheStore.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller));
      cacheStore.start();
      tableManager = (TableManager) ReflectionUtil.getValue(cacheStore, "tableManager");
   }

   @AfterMethod
   public void clearStore() throws Exception {
      cacheStore.clear();
      assertRowCount(0);
   }

   @AfterClass
   public void destroyStore() throws PersistenceException {
      cacheStore.stop();
      marshaller.stop();
   }

   /**
    * When trying to persist an unsupported object an exception is expected.
    */
   public void persistUnsupportedObject() throws Exception {
      try {
         cacheStore.write(new MarshalledEntryImpl("key", "value", null, marshaller));
         fail("exception is expected as PersonKey2StringMapper does not support strings");
      } catch (UnsupportedKeyTypeException e) {
         //expected
      }
      //just check that an person object will be persisted okay
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "Cluj Napoca", null, marshaller));
   }


   public void testStoreLoadRemove() throws Exception {
      assertRowCount(0);
      assertNull("should not be present in the store", cacheStore.load(MIRCEA));
      String value = "adsdsadsa";
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, value, null, marshaller));
      assertRowCount(1);
      assertEquals(value, cacheStore.load(MIRCEA).getValue());
      assertFalse(cacheStore.delete(MANIK));
      assertEquals(value, cacheStore.load(MIRCEA).getValue());
      assertRowCount(1);
      assertTrue(cacheStore.delete(MIRCEA));
      assertRowCount(0);
   }


   public void testClear() throws Exception {
      assertRowCount(0);
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "value", null, marshaller));
      cacheStore.write(new MarshalledEntryImpl(MANIK, "value", null, marshaller));
      assertRowCount(2);
      cacheStore.clear();
      assertRowCount(0);
   }

   public void testPurgeExpired() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create(MIRCEA, "val", 1000);
      InternalCacheEntry second = TestInternalCacheEntryFactory.create(MANIK, "val2");
      cacheStore.write(marshalledEntry(first, marshaller));
      cacheStore.write(marshalledEntry(second, marshaller));
      assertRowCount(2);
      Thread.sleep(1100);
      cacheStore.purge(new WithinThreadExecutor(), null);
      assertRowCount(1);
      assertEquals("val2", cacheStore.load(MANIK).getValue());
   }

   protected int rowCount() {
      ConnectionFactory connectionFactory = getConnection();
      TableName tableName = tableManager.getTableName();
      return UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
   }

   protected ConnectionFactory getConnection() {
      JdbcStringBasedStore store = (JdbcStringBasedStore) cacheStore;
      return store.getConnectionFactory();
   }

   protected void assertRowCount(int size) {
      assertEquals(size, rowCount());
   }
}
