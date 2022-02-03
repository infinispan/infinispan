package org.infinispan.persistence.file;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.SingleFile.SingleFileStoreTest")
public class SingleFileStoreTest extends BaseNonBlockingStoreTest {

   private String tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   private boolean segmented;

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   public SingleFileStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
              new SingleFileStoreTest().segmented(false),
              new SingleFileStoreTest().segmented(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder cb) {
      createCacheStoreConfig(cb.persistence());
      return cb.build();
   }

   protected SingleFileStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      SingleFileStoreConfigurationBuilder cfg = lcb.addStore(SingleFileStoreConfigurationBuilder.class);
      cfg.segmented(segmented);
      cfg.location(tmpDirectory);
      cfg.fragmentationFactor(0.5f);
      return cfg;
   }


   @Override
   protected NonBlockingStore createStore() {
      clearTempDir();
      return new SingleFileStore<>();
   }

   /**
    * Test to make sure that when segments are added or removed that there are no issues
    */
   public void testSegmentsRemovedAndAdded() {
      Object key = "first-key";
      Object value = "some-value";
      int segment = keyPartitioner.getSegment(key);

      if (!segmented) {
         Exceptions.expectException(UnsupportedOperationException.class,
                                    () -> store.removeSegments(IntSets.immutableSet(segment)));
         Exceptions.expectException(UnsupportedOperationException.class,
                                    () -> store.addSegments(IntSets.immutableSet(segment)));
         return;
      }

      InternalCacheEntry entry = TestInternalCacheEntryFactory.create(key, value);
      MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());

      store.write(me);

      assertTrue(store.contains(key));

      // Now remove the segment that held our key
      CompletionStages.join(store.removeSegments(IntSets.immutableSet(segment)));

      assertFalse(store.contains(key));

      // Now add the segment back
      CompletionStages.join(store.addSegments(IntSets.immutableSet(segment)));

      store.write(me);

      assertTrue(store.contains(key));
   }

   public void testStopDuringClear() {
      InternalCacheEntry entry = TestInternalCacheEntryFactory.create("key", "value");
      MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());

      store.write(me);
      CompletionStage<Void> clearStage = store.clear();
      store.stopAndWait();
      CompletionStages.join(clearStage);

      // The store must be able to start
      startStore(store);

      // But because clear does its work on a separate thread, it may run after stop
      long size = CompletionStages.join(store.size(IntSets.immutableRangeSet(segmentCount)));
      assertTrue(size == 0 || size == 1);
   }
}
