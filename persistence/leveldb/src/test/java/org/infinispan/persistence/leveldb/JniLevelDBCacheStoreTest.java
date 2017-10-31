package org.infinispan.persistence.leveldb;

import java.io.IOException;

import org.infinispan.commons.test.skip.SkipOnOs;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.leveldb.JniLevelDBCacheStoreTest")
@SkipOnOs({SkipOnOs.OS.SOLARIS, SkipOnOs.OS.WINDOWS})
public class JniLevelDBCacheStoreTest extends LevelDBStoreTest {

   protected LevelDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      LevelDBStoreConfigurationBuilder builder = super.createCacheStoreConfig(lcb);
      builder.implementationType(LevelDBStoreConfiguration.ImplementationType.JNI);
      return builder;
   }

   @Override
   public void testConcurrentWriteAndRestart() {
      super.testConcurrentWriteAndRestart();
   }

   @Override
   public void testConcurrentWriteAndStop() {
      super.testConcurrentWriteAndStop();
   }

   @Override
   public void testLoadAndStoreImmortal() throws PersistenceException {
      super.testLoadAndStoreImmortal();
   }

   @Override
   public void testLoadAndStoreWithLifespan() throws Exception {
      super.testLoadAndStoreWithLifespan();
   }

   @Override
   public void testWriteAndDeleteBatch() throws Exception {
      super.testWriteAndDeleteBatch();
   }

   @Override
   public void testLoadAndStoreWithIdle() throws Exception {
      super.testLoadAndStoreWithIdle();
   }

   @Override
   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      super.testLoadAndStoreWithLifespanAndIdle();
   }

   @Override
   public void testLoadAndStoreWithLifespanAndIdle2() throws Exception {
      super.testLoadAndStoreWithLifespanAndIdle2();
   }

   @Override
   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      super.testStopStartDoesNotNukeValues();
   }

   @Override
   public void testPreload() throws Exception {
      super.testPreload();
   }

   @Override
   public void testStoreAndRemove() throws PersistenceException {
      super.testStoreAndRemove();
   }

   @Override
   public void testPurgeExpired() throws Exception {
      super.testPurgeExpired();
   }

   @Override
   public void testLoadAll() throws PersistenceException {
      super.testLoadAll();
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      super.testReplaceExpiredEntry();
   }

   @Override
   public void testLoadAndStoreBytesValues() throws PersistenceException, IOException, InterruptedException {
      super.testLoadAndStoreBytesValues();
   }
}
