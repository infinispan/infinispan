package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.InitializationContextImpl;
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
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.persistence.BaseStoreTest.mockCache;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.testng.Assert.assertEquals;

/**
 * Tester for {@link JdbcStringBasedStore} with an alternative {@link org.infinispan.persistence.keymappers.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperTest")
public class JdbcStringBasedStoreAltMapperTest {

   AdvancedLoadWriteStore cacheStore;
   private TableManipulation tableManipulation;
   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);

   @BeforeTest
   public void createCacheStore() throws CacheLoaderException {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                  .key2StringMapper(PersonKey2StringMapper.class);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      cacheStore = new JdbcStringBasedStore();
      cacheStore.init(new InitializationContextImpl(storeBuilder.create(), mockCache(getClass().getName()), getMarshaller(),
                                                    new DefaultTimeService(), new ByteBufferFactoryImpl(), new MarshalledEntryFactoryImpl(getMarshaller())));
      cacheStore.start();
      tableManipulation = (TableManipulation) ReflectionUtil.getValue(cacheStore, "tableManipulation");
   }

   @AfterMethod
   public void clearStore() throws Exception {
      cacheStore.clear();
      assert rowCount() == 0;
   }

   @AfterTest
   public void destroyStore() throws CacheLoaderException {
      cacheStore.stop();
   }

   /**
    * When trying to persist an unsupported object an exception is expected.
    */
   public void persistUnsupportedObject() throws Exception {
      try {
         cacheStore.write(new MarshalledEntryImpl("key", "value", null, getMarshaller()));
         assert false : "exception is expected as PersonKey2StringMapper does not support strings";
      } catch (UnsupportedKeyTypeException e) {
         assert true : "expected";
      }
      //just check that an person object will be persisted okay
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "Cluj Napoca", null, getMarshaller()));
   }


   public void testStoreLoadRemove() throws Exception {
      assert rowCount() == 0;
      assert cacheStore.load(MIRCEA) == null : "should not be present in the store";
      String value = "adsdsadsa";
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, value, null, getMarshaller()));
      assert rowCount() == 1;
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert !cacheStore.delete(MANIK);
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert rowCount() == 1;
      assert cacheStore.delete(MIRCEA);
      assert rowCount() == 0;
   }


   public void testClear() throws Exception {
      assert rowCount() == 0;
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "value", null, getMarshaller()));
      cacheStore.write(new MarshalledEntryImpl(MANIK, "value", null, getMarshaller()));
      assert rowCount() == 2;
      cacheStore.clear();
      assert rowCount() == 0;
   }

   public void testPurgeExpired() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create(MIRCEA, "val", 1000);
      InternalCacheEntry second = TestInternalCacheEntryFactory.create(MANIK, "val2");
      cacheStore.write(marshalledEntry(first, getMarshaller()));
      cacheStore.write(marshalledEntry(second, getMarshaller()));
      assertEquals(rowCount(), 2);
      Thread.sleep(1100);
//      printTableContent();
      cacheStore.purge(new WithinThreadExecutor(), null);
      assert rowCount() == 1;
      assert cacheStore.load(MANIK).getValue().equals("val2");
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

   protected StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }
}
