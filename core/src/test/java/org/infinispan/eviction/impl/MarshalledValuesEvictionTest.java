package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
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
      cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void testEvictCustomKeyValue() {
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int) Util.random(2000);
         EvictionPojo p2 = new EvictionPojo();
         p2.i = 24;
         cache.put(p1, p2);
      }

      assertEquals(CACHE_SIZE, cache.getAdvancedCache().getDataContainer().size());
   }

   public void testEvictPrimitiveKeyCustomValue() {
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int) Util.random(2000);
         cache.put(i, p1);
      }
   }

   @SerializeWith(EvictionPojo.Externalizer.class)
   public static class EvictionPojo {
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

      public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<EvictionPojo> {
         @Override
         public void writeObject(ObjectOutput out, EvictionPojo pojo) throws IOException {
            out.writeInt(pojo.i);
         }

         @Override
         public EvictionPojo readObject(ObjectInput in) throws IOException, ClassNotFoundException {
            EvictionPojo pojo = new EvictionPojo();
            pojo.i = in.readInt();
            return pojo;
         }
      }

   }
}
