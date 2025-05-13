package org.infinispan.interceptors.impl;

import static org.infinispan.test.TestingUtil.mapOf;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test that tx modifications are properly applied in stores.
 *
 * <p>It's a unit test, but uses a full cache manager because it's easier to set up.
 * See ISPN-10022.</p>
 *
 * @since 10.0
 */
@Test(groups = "unit", testName = "interceptors.impl.CacheWriterTxModificationsTest")
public class CacheWriterTxModificationsTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC);
      config.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   public void testCommit() throws Throwable {
      FunctionalMapImpl<Object, Object> functionalMap = FunctionalMapImpl.create(cache.getAdvancedCache());
      FunctionalMap.WriteOnlyMap<Object, Object> woMap = WriteOnlyMapImpl.create(functionalMap);
      FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(functionalMap);
      DummyInMemoryStore store = TestingUtil.getFirstStore(cache);

      cache.putAll(mapOf("remove", "initial",
                         "replace", "initial",
                         "computeIfPresent", "initial",
                         "woRemove", "initial",
                         "rwRemove", "initial"));

      tm().begin();
      try {
         cache.put("put", "value");
         cache.putIfAbsent("putIfAbsent", "value");
         cache.remove("remove");
         cache.replace("replace", "value");
         cache.compute("compute", (k, v) -> "value");
         cache.computeIfAbsent("computeIfAbsent", k -> "value");
         cache.computeIfPresent("computeIfPresent", (k, v) -> "value");
         cache.putAll(mapOf("putAll", "value"));
         woMap.eval("woSet", entry -> entry.set("value"));
         woMap.eval("woRemove", entry -> entry.set("value"));
         woMap.eval("rwSet", entry -> entry.set("value"));
         woMap.eval("rwRemove", entry -> entry.set("value"));
      } finally {
         tm().commit();
      }

      DataContainer<Object, Object> dataContainer = cache.getAdvancedCache().getDataContainer();
      dataContainer.forEach(entry -> {
         MarshallableEntry storeEntry = store.loadEntry(entry.getKey());
         assertNotNull("Missing store entry: " + entry.getKey(), storeEntry);
         assertEquals(entry.getValue(), storeEntry.getValue());
      });
      store.keySet().forEach(k -> {
         assertEquals(store.loadEntry(k).getValue(), dataContainer.peek(k).getValue());
      });
   }
}
