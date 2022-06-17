package org.infinispan.persistence.rocksdb;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseTombstonePersistenceTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.Test;

/**
 * Tests tombstone stored in {@link RocksDBStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.rocksdb.RocksDBStoreTombstoneTest")
public class RocksDBStoreTombstoneTest extends BaseTombstonePersistenceTest {

   private static final String TMP_DIRECTORY = CommonsTestingUtil.tmpDirectory(MethodHandles.lookup().lookupClass());

   @Override
   protected WaitNonBlockingStore<String, String> getStore() {
      return wrapAndStart(new RocksDBStore<>(), createContext());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      return false;
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .location(TMP_DIRECTORY)
            .expiredLocation(TMP_DIRECTORY);
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }
}
