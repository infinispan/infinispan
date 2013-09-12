package org.infinispan.persistence.file;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.DummyInitializationContext;
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
      store.init(new DummyInitializationContext(fileStoreConfiguration, getCache(), getMarshaller(), new ByteBufferFactoryImpl(),
                                                new MarshalledEntryFactoryImpl(getMarshaller())));
      store.start();
      return store;
   }
}
