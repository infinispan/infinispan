package org.infinispan.jboss.marshalling.dataconversion;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.jboss.DataConversionTest")
public class DataConversionTest extends org.infinispan.dataconversion.DataConversionTest {

   @Test
   public void testObjectEncoder() {
      GenericJbossMarshallerEncoder encoder = new GenericJbossMarshallerEncoder(org.infinispan.dataconversion.DataConversionTest.class.getClassLoader());
      withCacheManager(new CacheManagerCallable(
            createCacheManager(new ConfigurationBuilder())) {

         GenericJBossMarshaller marshaller = new GenericJBossMarshaller();

         private byte[] marshall(Object o) {
            try {
               return marshaller.objectToByteBuffer(o);
            } catch (IOException | InterruptedException e) {
               throw new AssertionError("Cannot marshall content", e);
            }
         }

         @Override
         public void call() {
            GlobalComponentRegistry registry = cm.getGlobalComponentRegistry();
            EncoderRegistry encoderRegistry = registry.getComponent(EncoderRegistry.class);
            encoderRegistry.registerEncoder(encoder);

            cm.getClassAllowList().addClasses(Person.class);
            Cache<byte[], byte[]> cache = cm.getCache();

            // Write encoded content to the cache
            Person key1 = new Person("key1");
            Person value1 = new Person("value1");
            byte[] encodedKey1 = marshall(key1);
            byte[] encodedValue1 = marshall(value1);
            cache.put(encodedKey1, encodedValue1);

            // Read encoded content
            assertEquals(cache.get(encodedKey1), encodedValue1);

            // Read with a different valueEncoder
            AdvancedCache<Person, Person> encodingCache = (AdvancedCache<Person, Person>) cache.getAdvancedCache().withEncoding(GenericJbossMarshallerEncoder.class);

            assertEquals(encodingCache.get(key1), value1);
         }
      });

   }
}
