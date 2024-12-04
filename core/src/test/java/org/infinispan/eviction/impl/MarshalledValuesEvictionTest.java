package org.infinispan.eviction.impl;

import static org.infinispan.protostream.FileDescriptorSource.fromString;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoField;
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
      cacheManager = TestCacheManagerFactory.createCacheManager(new ContextInitializer(), cfg);
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void testEvictCustomKeyValue() {
      EvictionPojoMarshaller.resetStats();
      int expectedWrites = 0;
      int expectedReads = 0;
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int) Util.random(2000);
         EvictionPojo p2 = new EvictionPojo();
         p2.i = 24;
         Object old = cache.put(p1, p2);
         if (old != null)
            expectedReads++; // unmarshall old value if overwritten
         expectedWrites += 2; // key and value
      }

      assertEquals(CACHE_SIZE, cache.getAdvancedCache().getDataContainer().size());

      assertEquals(expectedWrites, EvictionPojoMarshaller.writes.get());
      assertEquals(expectedReads, EvictionPojoMarshaller.reads.get());
   }

   public void testEvictPrimitiveKeyCustomValue() {
      EvictionPojoMarshaller.resetStats();
      int expectedWrites = 0;
      int expectedReads = 0;
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int) Util.random(2000);
         Object old = cache.put(i, p1);
         if (old != null)
            expectedReads++; // unmarshall old value if overwritten
         expectedWrites++; // just the value
      }
      assertEquals(expectedWrites, EvictionPojoMarshaller.writes.get());
      assertEquals(expectedReads, EvictionPojoMarshaller.reads.get());
   }

   static class EvictionPojoMarshaller implements org.infinispan.protostream.MessageMarshaller<EvictionPojo> {
      static AtomicInteger writes = new AtomicInteger();
      static AtomicInteger reads = new AtomicInteger();

      public static void resetStats() {
         reads.set(0);
         writes.set(0);
      }

      @Override
      public EvictionPojo readFrom(ProtoStreamReader reader) throws IOException {
         EvictionPojo o = new EvictionPojo();
         o.i = reader.readInt("i");
         reads.incrementAndGet();
         return o;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, EvictionPojo evictionPojo) throws IOException {
         writer.writeInt("i", evictionPojo.i);
         writes.incrementAndGet();
      }

      @Override
      public Class<? extends EvictionPojo> getJavaClass() {
         return EvictionPojo.class;
      }

      @Override
      public String getTypeName() {
         return "EvictionPojo";
      }
   }

   private static class ContextInitializer implements SerializationContextInitializer {
      @Override
      public void registerSchema(SerializationContext serCtx) {
         serCtx.registerProtoFiles(fromString(EvictionPojo.class.getName(), "message EvictionPojo {optional int32 i=1;}"));
      }

      @Override
      public void registerMarshallers(SerializationContext serCtx) {
         serCtx.registerMarshaller(new EvictionPojoMarshaller());
      }
   }

   public static class EvictionPojo {
      @ProtoField(number = 1, defaultValue = "0")
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

   }
}
