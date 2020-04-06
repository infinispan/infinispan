package org.infinispan.persistence.dummy;

import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreTest")
public class DummyInMemoryStoreTest /*extends BaseStoreTest*/ {
   // TODO: need to repurpose or add a new base store test that new SPI implementors can extend
//
//   @Override
//   protected AdvancedLoadWriteStore createStore() throws PersistenceException {
//      DummyInMemoryStore store = new DummyInMemoryStore();
//      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
//      final DummyInMemoryStoreConfigurationBuilder loader = builder
//            .persistence()
//            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
//      loader
//         .storeName(getClass().getName());
//      store.init(createContext(builder.build()));
//      return store;
//   }
}
