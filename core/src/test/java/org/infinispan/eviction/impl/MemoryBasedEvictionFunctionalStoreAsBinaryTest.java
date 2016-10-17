package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Random;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.StorageType;
import org.infinispan.marshall.CustomClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MemoryBasedEvictionFunctionalStoreAsBinaryTest")
public class MemoryBasedEvictionFunctionalStoreAsBinaryTest extends MemoryBasedEvictionFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder cb) {
      super.configure(cb);
      cb.memory().storageType(StorageType.BINARY);
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
