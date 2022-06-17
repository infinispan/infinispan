package org.infinispan.persistence.sifs;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseTombstonePersistenceTest;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.Test;

/**
 * Tests tombstone stored in {@link NonBlockingSoftIndexFileStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreTombstoneTest")
public class SoftIndexFileStoreTombstoneTest extends BaseTombstonePersistenceTest {

   private static final String TMP_DIRECTORY = CommonsTestingUtil.tmpDirectory(MethodHandles.lookup().lookupClass());

   @Override
   protected WaitNonBlockingStore<String, String> getStore() throws Exception {
      return wrapAndStart(new NonBlockingSoftIndexFileStore<>(), createContext());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      // tombstone not present with "publishKeys()" is invoked.
      return false;
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      builder.persistence().addSoftIndexFileStore().indexLocation(TMP_DIRECTORY).dataLocation(TMP_DIRECTORY);
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }
}
