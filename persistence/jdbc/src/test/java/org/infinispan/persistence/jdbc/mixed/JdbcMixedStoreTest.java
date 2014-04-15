package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.TableName;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.persistence.jdbc.stringbased.Person;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;

import static org.infinispan.test.TestingUtil.internalMetadata;

/**
 * Tester class for {@link JdbcMixedStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.mixed.JdbcMixedStoreTest")
public class JdbcMixedStoreTest {

   private AdvancedLoadWriteStore cacheStore;
   private TableManipulation stringsTm;
   private TableManipulation binaryTm;
   private ConnectionFactoryConfig cfc;

   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);

   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;


   @BeforeMethod
   public void createCacheStore() throws PersistenceException {

      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = cc
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);
      storeBuilder
            .stringTable()
            .tableNamePrefix("STRINGS_TABLE")
            .key2StringMapper(DefaultTwoWayKey2StringMapper.class)
            .binaryTable()
            .tableNamePrefix("BINARY_TABLE");


      cacheManager = TestCacheManagerFactory.createCacheManager(cc);
      cache = cacheManager.getCache();

      cacheStore = TestingUtil.getFirstWriter(cache);
      stringsTm = (TableManipulation) ReflectionUtil.getValue(((JdbcMixedStore)cacheStore).getStringStore(), "tableManipulation");
      binaryTm = (TableManipulation) ReflectionUtil.getValue(((JdbcMixedStore)cacheStore).getBinaryStore(), "tableManipulation");
   }


   protected StreamingMarshaller getMarshaller() {
      return cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      cacheStore.clear();
      assertBinaryRowCount(0);
      assertStringsRowCount(0);
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testMixedStore() throws Exception {
      cacheStore.write(new MarshalledEntryImpl("String", "someValue", null, getMarshaller()));
      assertStringsRowCount(1);
      assertBinaryRowCount(0);
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "value", null, getMarshaller()));
      assertStringsRowCount(1);
      assertStringsRowCount(1);
      assert cacheStore.load(MIRCEA).getValue().equals("value");
      assert cacheStore.load("String").getValue().equals("someValue");
   }

   public void testMultipleEntriesWithSameHashCode() throws Exception {
      Person one = new Person("Mircea", "Markus", 28);
      Person two = new Person("Manik", "Surtani", 28);
      one.setHashCode(100);
      two.setHashCode(100);
      cacheStore.write(new MarshalledEntryImpl(one, "value", null, getMarshaller()));
      assertBinaryRowCount(1);
      assertStringsRowCount(0);
      cacheStore.write(new MarshalledEntryImpl(two, "otherValue",null, getMarshaller()));
      assertBinaryRowCount(1); //both go to same bucket
      assertStringsRowCount(0);
      assert cacheStore.load(one).getValue().equals("value");
      assert cacheStore.load(two).getValue().equals("otherValue");
   }

   public void testClear() throws Exception {
      cacheStore.write(new MarshalledEntryImpl("String", "someValue",null, getMarshaller()));
      assertRowCounts(0, 1);
      cacheStore.write(new MarshalledEntryImpl(MIRCEA, "value", null, getMarshaller()));
      assertRowCounts(1, 1);
      cacheStore.clear();
      assertRowCounts(0, 0);
   }

   private MarshalledEntryImpl marshalledEntry(Object key, Object value) {
      return new MarshalledEntryImpl(key, value, null, getMarshaller());
   }

   public void testLoadAll() throws Exception {
      MarshalledEntryImpl first = marshalledEntry("String", "someValue");
      MarshalledEntryImpl second = marshalledEntry("String2", "someValue");
      MarshalledEntryImpl third = marshalledEntry(MIRCEA, "value1");
      MarshalledEntryImpl forth = marshalledEntry(MANIK, "value2");
      cacheStore.write(first);
      cacheStore.write(second);
      cacheStore.write(third);
      cacheStore.write(forth);
      assertRowCounts(2, 2);
      Set<MarshalledEntry> entries = TestingUtil.allEntries(cacheStore);
      assert entries.size() == 4 : "Expected 4 and got: " + entries;
      assert entries.contains(first);
      assert entries.contains(second);
      assert entries.contains(third);
      assert entries.contains(forth);
   }


   public void testPurgeExpired() throws Exception {
      MarshalledEntryImpl first = new MarshalledEntryImpl("String", "someValue", internalMetadata(1000l, null), getMarshaller());
      MarshalledEntryImpl second = new MarshalledEntryImpl(MIRCEA, "value1", internalMetadata(1000l, null), getMarshaller());
      cacheStore.write(first);
      cacheStore.write(second);
      assertRowCounts(1, 1);
      Thread.sleep(1200);
      cacheStore.purge(new WithinThreadExecutor(), null);
      assertRowCounts(0, 0);
   }

   public void testPurgeExpiredWithRemainingEntries() throws Exception {
      MarshalledEntryImpl first = new MarshalledEntryImpl("String", "someValue", internalMetadata(1000l, null), getMarshaller());
      MarshalledEntryImpl second = marshalledEntry("String2", "someValue");
      MarshalledEntryImpl third = new MarshalledEntryImpl(MIRCEA, "value1", internalMetadata(1000l, null), getMarshaller());
      MarshalledEntryImpl forth = marshalledEntry(MANIK, "value1");;

      cacheStore.write(first);
      cacheStore.write(second);
      cacheStore.write(third);
      cacheStore.write(forth);
      assertRowCounts(2, 2);
      Thread.sleep(1200);
      cacheStore.purge(new WithinThreadExecutor(), null);
      assertRowCounts(1, 1);
   }

   private void assertRowCounts(int binary, int strings) {
      assertBinaryRowCount(binary);
      assertStringsRowCount(strings);
   }

   private void assertStringsRowCount(int rowCount) {
      JdbcMixedStore store = (JdbcMixedStore) cacheStore;
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = stringsTm.getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }

   private void assertBinaryRowCount(int rowCount) {
      JdbcMixedStore store = (JdbcMixedStore) cacheStore;
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = binaryTm.getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }
}
