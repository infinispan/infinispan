package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Tests that invalidation and lazy deserialization works as expected.
 *
 * @author Galder Zamarreño
 * @since 4.2
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
      // Each cache manager attempts to serialize the pojo once to check is
      // marshallable, so add a couple of more times on previous 3. Note that
      // this is only done once for the type.
      assertSerializationCounts(5, 0);
      cache1.put(key, "3");
      // +2 carried on here.
      assertSerializationCounts(6, 0);
   }

   public static class InvalidatedPojo implements Externalizable {
      final Log log = LogFactory.getLog(InvalidatedPojo.class);

      static int invalidSerializationCount, invalidDeserializationCount;

      public int updateSerializationCount() {
         return ++invalidSerializationCount;
      }

      public int updateDeserializationCount() {
         return ++invalidDeserializationCount;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         int serCount = updateSerializationCount();
         log.trace("invalidSerializationCount=" + serCount);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         int deserCount = updateDeserializationCount();
         log.trace("invalidDeserializationCount=" + deserCount);
      }
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assert InvalidatedPojo.invalidSerializationCount == serializationCount : "Serialization count: expected " + serializationCount + " but was " + InvalidatedPojo.invalidSerializationCount;
      assert InvalidatedPojo.invalidDeserializationCount == deserializationCount : "Deserialization count: expected " + deserializationCount + " but was " + InvalidatedPojo.invalidDeserializationCount;
   }

}
