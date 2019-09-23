package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.CountMarshallingPojo;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MarshalledValuesEvictionTest")
public class MarshalledValuesEvictionTest extends SingleCacheManagerTest {

   private static final int CACHE_SIZE = 128;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().size(CACHE_SIZE).evictionType(EvictionType.COUNT).storageType(StorageType.BINARY)
            .expiration().wakeUpInterval(100L)
            .locking().useLockStriping(false) // to minimise chances of deadlock in the unit test
            .build();
      cacheManager = TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, cfg);
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void testEvictCustomKeyValue() {
      CountMarshallingPojo.reset();
      int expectedWrites = 0;
      int expectedReads = 0;
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         CountMarshallingPojo p1 = new CountMarshallingPojo(Util.random(2000));
         CountMarshallingPojo p2 = new CountMarshallingPojo(24);
         Object old = cache.put(p1, p2);
         if (old != null)
            expectedReads++; // unmarshall old value if overwritten
         expectedWrites += 2; // key and value
      }

      assertEquals(CACHE_SIZE, cache.getAdvancedCache().getDataContainer().size());
      assertEquals(expectedWrites, CountMarshallingPojo.getMarshallCount());
      assertEquals(expectedReads, CountMarshallingPojo.getUnmarshallCount());
   }

   public void testEvictPrimitiveKeyCustomValue() {
      CountMarshallingPojo.reset();
      int expectedWrites = 0;
      int expectedReads = 0;
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         CountMarshallingPojo p1 = new CountMarshallingPojo(Util.random(2000));
         Object old = cache.put(i, p1);
         if (old != null)
            expectedReads++; // unmarshall old value if overwritten
         expectedWrites++; // just the value
      }
      assertEquals(expectedWrites, CountMarshallingPojo.getMarshallCount());
      assertEquals(expectedReads, CountMarshallingPojo.getUnmarshallCount());
   }
}
