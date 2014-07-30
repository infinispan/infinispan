package org.infinispan.persistence.file;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;

/**
 * Low level single-file cache store tests.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileStoreTest")
public class SingleFileStoreTest extends BaseStoreTest {

   String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      SingleFileStore store = new SingleFileStore();
      ConfigurationBuilder configurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      configurationBuilder
            .persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory);
      store.init(createContext(configurationBuilder.build()));
      return store;
   }
}
