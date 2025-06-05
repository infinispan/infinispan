package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.CountMarshallingPojo;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MarshalledValuesManualEvictionTest")
public class MarshalledValuesManualEvictionTest extends SingleCacheManagerTest {

   public static final String POJO_NAME = MarshalledValuesManualEvictionTest.class.getName();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.memory().storage(StorageType.HEAP).encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testManualEvictCustomKeyValue() {
      CountMarshallingPojo.reset(POJO_NAME);
      CountMarshallingPojo p1 = new CountMarshallingPojo(POJO_NAME, 64);
      CountMarshallingPojo p2 = new CountMarshallingPojo(POJO_NAME, 24);
      CountMarshallingPojo p3 = new CountMarshallingPojo(POJO_NAME, 97);
      CountMarshallingPojo p4 = new CountMarshallingPojo(POJO_NAME, 35);

      cache.put(p1, p2); // 2 writes, key & value
      cache.put(p3, p4); // 2 writes, key & value
      assertEquals(2, cache.size());
      cache.evict(p1); // 1 write
      assertEquals(1, cache.size());
      assertEquals(p4, cache.get(p3)); // 1 write, 1 read
      assertEquals(6, CountMarshallingPojo.getMarshallCount(POJO_NAME));
      assertEquals(1, CountMarshallingPojo.getUnmarshallCount(POJO_NAME));
   }

   public void testEvictPrimitiveKeyCustomValue() {
      CountMarshallingPojo.reset(POJO_NAME);
      CountMarshallingPojo p1 = new CountMarshallingPojo(POJO_NAME, 51);
      CountMarshallingPojo p2 = new CountMarshallingPojo(POJO_NAME, 78);

      cache.put("key-isoprene", p1); // 1 write
      cache.put("key-hexastyle", p2); // 1 write
      assertEquals(2, cache.size());
      cache.evict("key-isoprene");
      assertEquals(1, cache.size());
      assertEquals(p2, cache.get("key-hexastyle")); // 1 read
      assertEquals(2, CountMarshallingPojo.getMarshallCount(POJO_NAME));
      assertEquals(1, CountMarshallingPojo.getUnmarshallCount(POJO_NAME));
   }

}
