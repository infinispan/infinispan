package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Random;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.marshall.CustomClass;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.jgroups.util.UUID;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MemoryBasedEvictionFunctionalStoreAsBinaryTest")
public class MemoryBasedEvictionFunctionalStoreAsBinaryTest extends MemoryBasedEvictionFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder cb) {
      super.configure(cb);
      cb.memory().storageType(storageType);
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new MemoryBasedEvictionFunctionalStoreAsBinaryTest().storageType(StorageType.BINARY),
      };
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

   public void testJGroupsAddress() {
      cache.put("key", new JGroupsAddress(new UUID()));
   }
}
