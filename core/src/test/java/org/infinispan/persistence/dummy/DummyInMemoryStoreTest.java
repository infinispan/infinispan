package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreTest")
public class DummyInMemoryStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws PersistenceException {
      DummyInMemoryStore store = new DummyInMemoryStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      final DummyInMemoryStoreConfigurationBuilder loader = builder
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      loader
         .storeName(getClass().getName());
      store.init(createContext(builder.build()));
      return store;
   }
}
