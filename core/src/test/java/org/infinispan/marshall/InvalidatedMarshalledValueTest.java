package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.infinispan.marshall.MarshalledValueTest.*;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarre�o
 * @since // TODO
 */
@Test(groups = "functional", testName = "marshall.InvalidatedMarshalledValueTest")
public class InvalidatedMarshalledValueTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      Cache cache1, cache2;
      Configuration invlSync = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      invlSync.setUseLazyDeserialization(true);

      createClusteredCaches(2, "invlSync", invlSync);

      cache1 = cache(0, "invlSync");
      cache2 = cache(1, "invlSync");

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
   }

   private void assertMarshalledValueInterceptorPresent(Cache c) {
      InterceptorChain ic1 = TestingUtil.extractComponent(c, InterceptorChain.class);
      assert ic1.containsInterceptorType(MarshalledValueInterceptor.class);
   }

   public void testModificationsOnSameCustomKey() {
      Cache cache1 = cache(0, "invlSync");
      Cache cache2 = cache(1, "invlSync");

      InvalidatedPojo key = new InvalidatedPojo();
      cache2.put(key, "1");
      cache1.put(key, "2");
      assertSerializationCounts(3, 0);
      cache1.put(key, "3");
      assertSerializationCounts(4, 0);
   }

   public static class InvalidatedPojo extends Pojo {
      static int serializationCount, deserializationCount;

      @Override
      public int updateSerializationCount() {
         return ++serializationCount;
      }

      @Override
      public int updateDeserializationCount() {
         return ++deserializationCount;
      }
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assert InvalidatedPojo.serializationCount == serializationCount : "Serialization count: expected " + serializationCount + " but was " + InvalidatedPojo.serializationCount;
      assert InvalidatedPojo.deserializationCount == deserializationCount : "Deserialization count: expected " + deserializationCount + " but was " + InvalidatedPojo.deserializationCount;
   }

}
