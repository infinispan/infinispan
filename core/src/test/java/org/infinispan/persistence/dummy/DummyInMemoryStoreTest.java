package org.infinispan.persistence.dummy;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreTest")
public class DummyInMemoryStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws CacheLoaderException {
      DummyInMemoryStore cl = new DummyInMemoryStore();
      final DummyInMemoryStoreConfigurationBuilder loader = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      loader
         .storeName(getClass().getName());
      cl.init(new DummyInitializationContext(loader.create(), getCache(), getMarshaller(), new ByteBufferFactoryImpl(),
                                             new MarshalledEntryFactoryImpl(getMarshaller())));
      cl.start();
      csc = loader.create();
      return cl;
   }
}
