package org.infinispan.eviction.impl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "eviction.MarshalledValuesManualEvictionTest")
public class MarshalledValuesManualEvictionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.storeAsBinary().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testManualEvictCustomKeyValue() {
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 64;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 24;
      ManualEvictionPojo p3 = new ManualEvictionPojo();
      p3.i = 97;
      ManualEvictionPojo p4 = new ManualEvictionPojo();
      p4.i = 35;

      cache.put(p1, p2);
      cache.put(p3, p4);
      assertEquals(2, cache.size());
      cache.evict(p1);
      assertEquals(1, cache.size());
      assertEquals(p4, cache.get(p3));
   }

   public void testEvictPrimitiveKeyCustomValue() {
      ManualEvictionPojo p1 = new ManualEvictionPojo();
      p1.i = 51;
      ManualEvictionPojo p2 = new ManualEvictionPojo();
      p2.i = 78;

      cache.put("key-isoprene", p1);
      cache.put("key-hexastyle", p2);
      assertEquals(2, cache.size());
      cache.evict("key-isoprene");
      assertEquals(1, cache.size());
      assertEquals(p2, cache.get("key-hexastyle"));
   }

   public static class ManualEvictionPojo implements Externalizable {
      int i;

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
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
      }
   }

}
