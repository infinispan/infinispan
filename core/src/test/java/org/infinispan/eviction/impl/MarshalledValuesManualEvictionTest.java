package org.infinispan.eviction.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "eviction.MarshalledValuesManualEvictionTest")
public class MarshalledValuesManualEvictionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().storageType(StorageType.BINARY);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testManualEvictCustomKeyValue() {
      ManualEvictionPojo.resetStats();
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 64;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 24;
      ManualEvictionPojo p3 = new ManualEvictionPojo();
      p3.i = 97;
      ManualEvictionPojo p4 = new ManualEvictionPojo();
      p4.i = 35;

      cache.put(p1, p2); // 2 writes, key & value
      cache.put(p3, p4); // 2 writes, key & value
      assertEquals(2, cache.size());
      cache.evict(p1); // 1 write
      assertEquals(1, cache.size());
      assertEquals(p4, cache.get(p3)); // 1 write, 1 read
      assertEquals(6, ManualEvictionPojo.writes.get());
      assertEquals(1, ManualEvictionPojo.reads.get());
   }

   public void testEvictPrimitiveKeyCustomValue() {
      ManualEvictionPojo.resetStats();
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 51;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 78;

      cache.put("key-isoprene", p1); // 1 write
      cache.put("key-hexastyle", p2); // 1 write
      assertEquals(2, cache.size());
      cache.evict("key-isoprene");
      assertEquals(1, cache.size());
      assertEquals(p2, cache.get("key-hexastyle")); // 1 read
      assertEquals(2, ManualEvictionPojo.writes.get());
      assertEquals(1, ManualEvictionPojo.reads.get());
   }

   public static class ManualEvictionPojo implements Externalizable, ExternalPojo {
      static AtomicInteger writes = new AtomicInteger();
      static AtomicInteger reads = new AtomicInteger();
      int i;

      public static void resetStats() {
         reads.set(0);
         writes.set(0);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ManualEvictionPojo pojo = (ManualEvictionPojo) o;
         if (i != pojo.i) return false;
         return true;
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
         writes.incrementAndGet();
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         reads.incrementAndGet();
      }
   }

}
