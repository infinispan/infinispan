package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.CountMarshallingPojo;
import org.testng.annotations.Test;

/**
 * Tests that invalidation and lazy deserialization works as expected.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "marshall.InvalidatedMarshalledValueTest")
public class InvalidatedMarshalledValueTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder invlSync = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      invlSync.memory().storageType(StorageType.BINARY);

      createClusteredCaches(2, "invlSync", TestDataSCI.INSTANCE, invlSync);
      CountMarshallingPojo.reset();
   }

   public void testModificationsOnSameCustomKey() {
      Cache<CountMarshallingPojo, String> cache1 = cache(0, "invlSync");
      Cache<CountMarshallingPojo, String> cache2 = cache(1, "invlSync");

      CountMarshallingPojo key = new CountMarshallingPojo();
      cache2.put(key, "1");
      cache1.put(key, "2");
      // Marshalling is done eagerly now, so no need for extra serialization checks
      assertSerializationCounts(2, 0);
      cache1.put(key, "3");
      // +2 carried on here.
      assertSerializationCounts(3, 0);
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assert CountMarshallingPojo.getMarshallCount() == serializationCount : "Marshall count: expected " + serializationCount + " but was " + CountMarshallingPojo.getMarshallCount();
      assert CountMarshallingPojo.getUnmarshallCount() == deserializationCount : "Unmarshall count: expected " + deserializationCount + " but was " + CountMarshallingPojo.getUnmarshallCount();
   }
}
