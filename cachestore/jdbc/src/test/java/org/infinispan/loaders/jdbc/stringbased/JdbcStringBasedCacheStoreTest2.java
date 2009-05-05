package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.marshall.ObjectStreamMarshaller;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tester for {@link JdbcStringBasedCacheStore} with an alternative {@link org.infinispan.loaders.jdbc.stringbased.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreTest2")
public class JdbcStringBasedCacheStoreTest2 {

   CacheStore cacheStore;
   private ConnectionFactoryConfig cfc;
   private TableManipulation tableManipulation;
   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);

   @BeforeTest
   public void createCacheStore() throws CacheLoaderException {
      tableManipulation = UnitTestDatabaseManager.buildDefaultTableManipulation();
      cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(cfc, tableManipulation);
      config.setKey2StringMapperClass(PersonKey2StringMapper.class.getName());
      config.setPurgeSynchronously(true);
      cacheStore = new JdbcStringBasedCacheStore();
      cacheStore.init(config, null, new ObjectStreamMarshaller());
      cacheStore.start();
   }

   @AfterMethod
   public void clearStore() throws Exception {
      cacheStore.clear();
      assert rowCount() == 0;
   }

   @AfterTest
   public void destroyStore() throws CacheLoaderException {
      cacheStore.stop();
      UnitTestDatabaseManager.shutdownInMemoryDatabase(cfc);
   }

   /**
    * When trying to perists an unsupported object an exception is expected.
    */
   public void persistUnsupportedObject() throws Exception {
      try {
         cacheStore.store(InternalEntryFactory.create("key", "value"));
         assert false : "exception is expected as PersonKey2StringMapper does not support strings";
      } catch (UnsupportedKeyTypeException e) {
         assert true : "expected";
      }
      //just check that an person object will be persisted okay
      cacheStore.store(InternalEntryFactory.create(MIRCEA, "Cluj Napoca"));
   }


   public void testStoreLoadRemove() throws Exception {
      assert rowCount() == 0;
      assert cacheStore.load(MIRCEA) == null : "should not be present in the store";
      String value = "adsdsadsa";
      cacheStore.store(InternalEntryFactory.create(MIRCEA, value));
      assert rowCount() == 1;
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert !cacheStore.remove(MANIK);
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert rowCount() == 1;
      assert cacheStore.remove(MIRCEA);
      assert rowCount() == 0;
   }

   public void testRemoveAll() throws Exception {
      assert rowCount() == 0;
      cacheStore.store(InternalEntryFactory.create(MIRCEA, "value"));
      cacheStore.store(InternalEntryFactory.create(MANIK, "value"));
      assert rowCount() == 2;
      cacheStore.removeAll(Collections.singleton((Object) MIRCEA));
      assert cacheStore.load(MANIK).getValue().equals("value");
      assert rowCount() == 1;
      cacheStore.store(InternalEntryFactory.create(MIRCEA, "value"));
      assert rowCount() == 2;
      Set<Object> toRemove = new HashSet<Object>();
      toRemove.add(MIRCEA);
      toRemove.add(MANIK);
      cacheStore.removeAll(toRemove);
      assert rowCount() == 0;
   }

   public void testClear() throws Exception {
      assert rowCount() == 0;
      cacheStore.store(InternalEntryFactory.create(MIRCEA, "value"));
      cacheStore.store(InternalEntryFactory.create(MANIK, "value"));
      assert rowCount() == 2;
      cacheStore.clear();
      assert rowCount() == 0;
   }

   public void testPurgeExpired() throws Exception {
      InternalCacheEntry first = InternalEntryFactory.create(MIRCEA, "val", 1000);
      InternalCacheEntry second = InternalEntryFactory.create(MANIK, "val2");
      cacheStore.store(first);
      cacheStore.store(second);
      assert rowCount() == 2;
      Thread.sleep(1100);
//      printTableContent();
      cacheStore.purgeExpired();
      assert rowCount() == 1;
      assert cacheStore.load(MANIK).getValue().equals("val2");
   }

   private int rowCount() {
      ConnectionFactory connectionFactory = getConnection();
      String tableName = tableManipulation.getTableName();
      return UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
   }

   private ConnectionFactory getConnection() {

      JdbcStringBasedCacheStore store = (JdbcStringBasedCacheStore) cacheStore;
      return store.getConnectionFactory();
   }
}
