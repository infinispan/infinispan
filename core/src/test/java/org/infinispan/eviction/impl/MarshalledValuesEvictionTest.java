package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

@Test(groups = "unstable", testName = "eviction.MarshalledValuesEvictionTest",
      description = "See ISPN-4042. Is this test even valid?  Evictions don't go thru the " +
            "marshalled value interceptor when initiated form the data container! " +
            "-- original group: functional")
public class MarshalledValuesEvictionTest extends SingleCacheManagerTest {

   private static final int CACHE_SIZE=128;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.eviction().strategy(EvictionStrategy.LRU).maxEntries(CACHE_SIZE) // CACHE_SIZE max entries
         .expiration().wakeUpInterval(100L)
         .locking().useLockStriping(false) // to minimise chances of deadlock in the unit test
         .storeAsBinary().enable()
         .build();
      cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void testEvictCustomKeyValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int)Util.random(2000);
         EvictionPojo p2 = new EvictionPojo();
         p2.i = 24;
         cache.put(p1, p2);
      }

      assertEquals(CACHE_SIZE, cache.getAdvancedCache().getDataContainer().size());
   }

   public void testEvictPrimitiveKeyCustomValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int)Util.random(2000);
         cache.put(i, p1);
      }
   }

   public static class EvictionPojo implements Externalizable {
      int i;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         EvictionPojo pojo = (EvictionPojo) o;
         return i == pojo.i;
      }

      @Override
      public int hashCode() {
         int result;
         result = i;
         return result;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
      }

   }
}
