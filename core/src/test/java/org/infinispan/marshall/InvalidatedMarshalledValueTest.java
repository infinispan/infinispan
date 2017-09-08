package org.infinispan.marshall;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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

      createClusteredCaches(2, "invlSync", invlSync);
   }

   public void testModificationsOnSameCustomKey() {
      Cache<InvalidatedPojo, String> cache1 = cache(0, "invlSync");
      Cache<InvalidatedPojo, String> cache2 = cache(1, "invlSync");

      InvalidatedPojo key = new InvalidatedPojo();
      cache2.put(key, "1");
      cache1.put(key, "2");
      // Marshalling is done eagerly now, so no need for extra serialization checks
      assertSerializationCounts(2, 0);
      cache1.put(key, "3");
      // +2 carried on here.
      assertSerializationCounts(3, 0);
   }

   public static class InvalidatedPojo implements Externalizable, ExternalPojo {
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
