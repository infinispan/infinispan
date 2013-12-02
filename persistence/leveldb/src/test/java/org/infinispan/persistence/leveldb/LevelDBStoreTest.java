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

   @Override
   public void testPurgeExpired() throws Exception {
      long lifespan = 1000;
      InternalCacheEntry k1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry k2 = TestInternalCacheEntryFactory.create("k2", "v2", lifespan);
      InternalCacheEntry k3 = TestInternalCacheEntryFactory.create("k3", "v3", lifespan);
      cl.write(TestingUtil.marshalledEntry(k1, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(k2, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(k3, getMarshaller()));
      assert cl.contains("k1");
      assert cl.contains("k2");
      assert cl.contains("k3");
      Thread.sleep(lifespan + 100);
      cl.purge(new WithinThreadExecutor(), null);
      LevelDBStore fcs = (LevelDBStore) cl;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   public void testStopStartDoesntNukeValues() throws InterruptedException {
      assert !cl.contains("k1");
      assert !cl.contains("k2");

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", "v2");
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", "v3", -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", "v4", lifespan, idle);

      cl.write(TestingUtil.marshalledEntry(se1, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se2, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se3, getMarshaller()));
      cl.write(TestingUtil.marshalledEntry(se4, getMarshaller()));
      Thread.sleep(100);
      // Force a purge expired so that expiry tree is updated
      cl.purge(new WithinThreadExecutor(), null);
      cl.stop();
      cl.start();
      assert se1.isExpired();
      assert cl.load("k1") == null;
      assert !cl.contains("k1");
      assert cl.load("k2") != null;
      assert cl.contains("k2");
      assert cl.load("k2").getValue().equals("v2");
      assert se3.isExpired();
      assert cl.load("k3") == null;
      assert !cl.contains("k3");
      assert se3.isExpired();
      assert cl.load("k3") == null;
      assert !cl.contains("k3");
   }
}
