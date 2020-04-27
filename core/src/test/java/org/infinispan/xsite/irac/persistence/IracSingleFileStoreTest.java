package org.infinispan.xsite.irac.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.file.SingleFileStore;
import org.testng.annotations.Test;

/**
 * Tests if the IRAC metadata is properly stored and retrieved from a {@link SingleFileStore}.
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
@Test(groups = "functional", testName = "xsite.irac.persistence.IracSingleFileStoreTest")
public class IracSingleFileStoreTest extends BaseIracPersistenceTest<String> {


   public IracSingleFileStoreTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.persistence().addSingleFileStore().location(tmpDirectory);
   }
}
