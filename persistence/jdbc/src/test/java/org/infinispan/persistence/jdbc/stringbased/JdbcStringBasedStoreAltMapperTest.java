package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.TableName;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.testng.AssertJUnit.*;

/**
 * Tester for {@link JdbcStringBasedStore} with an alternative {@link org.infinispan.persistence.keymappers.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperTest")
public class JdbcStringBasedStoreAltMapperTest {

   private AdvancedLoadWriteStore cacheStore;
   private TableManipulation tableManipulation;
   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);
   private StreamingMarshaller marshaller;

   @BeforeTest
   public void createCacheStore() throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                  .key2StringMapper(PersonKey2StringMapper.class);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      cacheStore = new JdbcStringBasedStore();
      marshaller = new TestObjectStreamMarshaller();
      cacheStore.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller));
      cacheStore.start();
      tableManipulation = (TableManipulation) ReflectionUtil.getValue(cacheStore, "tableManipulation");
   }

   @AfterMethod
   public void clearStore() throws Exception {
      cacheStore.clear();
      assertRowCount(0);
   }

   @AfterTest
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
//      printTableContent();
      cacheStore.purge(new WithinThreadExecutor(), null);
      assertRowCount(1);
      assertEquals("val2", cacheStore.load(MANIK).getValue());
   }

   private int rowCount() {
      ConnectionFactory connectionFactory = getConnection();
      TableName tableName = tableManipulation.getTableName();
      return UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
   }

   private ConnectionFactory getConnection() {
      JdbcStringBasedStore store = (JdbcStringBasedStore) cacheStore;
      return store.getConnectionFactory();
   }

   private void assertRowCount(int size) {
      assertEquals(size, rowCount());
   }
}
