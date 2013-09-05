package org.infinispan.persistence.file;

import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.DummyLoaderContext;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.file.SingleFileStore;
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
@Test(groups = "unit", testName = "loaders.file.SingleFileCacheStoreTest")
public class SingleFileCacheStoreTest extends BaseCacheStoreTest {

   SingleFileStore store;
   String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      store = new SingleFileStore();
      SingleFileStoreConfiguration fileStoreConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory)
                  .create();
      store.init(new DummyLoaderContext(fileStoreConfiguration, getCache(), getMarshaller()));
      store.start();
      return store;
   }
}
