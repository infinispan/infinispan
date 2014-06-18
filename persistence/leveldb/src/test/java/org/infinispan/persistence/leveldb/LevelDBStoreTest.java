package org.infinispan.persistence.leveldb;

import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

@Test(groups = "unit", testName = "persistence.leveldb.LevelDBStoreTest")
public class LevelDBStoreTest extends BaseStoreTest {

   private LevelDBStore fcs;
   private String tmpDirectory;
   private EmbeddedCacheManager cacheManager;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   protected LevelDBStoreConfiguration createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      cacheManager = TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false);
      LevelDBStoreConfigurationBuilder cfg = new LevelDBStoreConfigurationBuilder(lcb);
      cfg.location(tmpDirectory + "/data");
      cfg.expiredLocation(tmpDirectory + "/expiry");
      cfg.clearThreshold(2);
      return cfg.create();
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return cacheManager.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @AfterMethod
   @Override
   public void tearDown() throws PersistenceException {
      super.tearDown();
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      fcs = new LevelDBStore();
      ConfigurationBuilder cb = new ConfigurationBuilder();
      LevelDBStoreConfiguration cfg = createCacheStoreConfig(cb.persistence());
      fcs.init(new DummyInitializationContext(cfg, getCache(), getMarshaller(), new ByteBufferFactoryImpl(),
                                              new MarshalledEntryFactoryImpl(getMarshaller())));
      fcs.start();
      return fcs;
   }
}
