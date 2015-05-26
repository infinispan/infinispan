package org.infinispan.eviction.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.CustomClass;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Random;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "eviction.MemoryBasedEvictionFunctionalStoreAsBinaryTest")
public class MemoryBasedEvictionFunctionalStoreAsBinaryTest extends MemoryBasedEvictionFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder cb) {
      super.configure(cb);
      cb.storeAsBinary().enable().storeKeysAsBinary(true).storeValuesAsBinary(true);
   }

   public void testCustomClass() throws Exception {
      long numberInserted = CACHE_SIZE / 10;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (float i = 0; i < numberInserted; i++) {
         cache.put(new CustomClass(randomStringFullOfInt(random, 10)),
                 new CustomClass(randomStringFullOfInt(random, 10)));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }
}
