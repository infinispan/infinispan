package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseTombstonePersistenceTest;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.Test;

/**
 * Tests tombstone stored in {@link DummyInMemoryStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.dummy.DummyInMemoryStoreTombstoneTest")
public class DummyInMemoryStoreTombstoneTest extends BaseTombstonePersistenceTest {

   @Override
   protected WaitNonBlockingStore<String, String> getStore() throws Exception {
      //noinspection unchecked
      return wrapAndStart(new DummyInMemoryStore("DummyInMemoryStoreTombstoneTest"), createContext());
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName("DummyInMemoryStoreTombstoneTest");
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      return false;
   }

}
