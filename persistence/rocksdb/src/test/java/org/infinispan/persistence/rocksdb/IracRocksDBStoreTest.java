package org.infinispan.persistence.rocksdb;

import java.io.File;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * Tests if the IRAC metadata is properly stored and retrieved from a {@link RocksDBStore}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.rocksdb.IracRocksDBStoreTest")
public class IracRocksDBStoreTest extends BaseIracPersistenceTest<String> {

   public IracRocksDBStoreTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .location(tmpDirectory + File.separator + "data")
            .expiredLocation(tmpDirectory + File.separator + "expiry");
   }
}
