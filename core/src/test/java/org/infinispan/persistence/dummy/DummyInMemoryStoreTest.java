package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreTest")
public class DummyInMemoryStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName());

      DummyInMemoryStore store = new DummyInMemoryStore();
      store.init(createContext(builder.build()));
      return store;
   }
}
