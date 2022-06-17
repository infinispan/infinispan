package org.infinispan.persistence.file;

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
 * Tests tombstone stored in {@link SingleFileStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.file.SingleFileStoreTombstoneTest")
public class SingleFileStoreTombstoneTest extends BaseTombstonePersistenceTest {

   private static final String TMP_DIRECTORY = CommonsTestingUtil.tmpDirectory(MethodHandles.lookup().lookupClass());

   @Override
   protected WaitNonBlockingStore<String, String> getStore() throws Exception {
      return wrapAndStart(new SingleFileStore<>(), createContext());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      return true;
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      builder.persistence().addSingleFileStore().location(TMP_DIRECTORY);
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }

}
