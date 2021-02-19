package org.infinispan.persistence.sifs;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * Tests if the IRAC metadata is properly stored and retrieved from a {@link SoftIndexFileStore}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.sifs.IracSIFSStoreTest")
public class IracSIFSStoreTest extends BaseIracPersistenceTest<String> {

   public IracSIFSStoreTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .segmented(false)
            .dataLocation(tmpDirectory)
            .indexLocation(tmpDirectory);
   }
}
