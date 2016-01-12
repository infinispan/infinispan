package org.infinispan.eviction.impl;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicBoolean;

@Test(groups = "unstable", testName = "eviction.MarshalledValuesEvictionTest",
      description = "See ISPN-4042. Is this test even valid?  Evictions don't go thru the " +
            "marshalled value interceptor when initiated form the data container! " +
            "-- original group: functional")
public class MarshalledValuesEvictionTest extends SingleCacheManagerTest {

   private static final int CACHE_SIZE=128;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.eviction().strategy(EvictionStrategy.LRU).maxEntries(CACHE_SIZE) // CACHE_SIZE max entries
         .expiration().wakeUpInterval(100L)
         .locking().useLockStriping(false) // to minimise chances of deadlock in the unit test
         .storeAsBinary().enable()
         .build();
      cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cacheManager.getCache();
      StreamingMarshaller marshaller = TestingUtil.extractComponent(cache, StreamingMarshaller.class);
      MockMarshalledValueInterceptor interceptor = new MockMarshalledValueInterceptor(marshaller);
      assert TestingUtil.replaceInterceptor(cache, interceptor, MarshalledValueInterceptor.class);
      return cacheManager;
   }

   public void testEvictCustomKeyValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int)Util.random(2000);
         EvictionPojo p2 = new EvictionPojo();
         p2.i = 24;
         cache.put(p1, p2);
      }

      // wait for the cache size to drop to CACHE_SIZE, up to a specified amount of time.
      long giveupTime = System.currentTimeMillis() + (1000 * 10); // 10 sec
      while (cache.getAdvancedCache().getDataContainer().size() > CACHE_SIZE && System.currentTimeMillis() < giveupTime) {
         TestingUtil.sleepThread(100);
      }

      assert cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE : "Expected 1, was " + cache.size();

      //let eviction manager kick in
      Util.sleep(3000);
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated.get();
   }

   public void testEvictPrimitiveKeyCustomValue() {
      for (int i = 0; i<CACHE_SIZE*2;i++) {
         EvictionPojo p1 = new EvictionPojo();
         p1.i = (int)Util.random(2000);
         EvictionPojo p2 = new EvictionPojo();
         p2.i = 24;
         cache.put(p1, p2);
      }

      // wait for the cache size to drop to CACHE_SIZE, up to a specified amount of time.
      long giveupTime = System.currentTimeMillis() + (1000 * 10); // 10 sec
      while (cache.getAdvancedCache().getDataContainer().size() > CACHE_SIZE && System.currentTimeMillis() < giveupTime) {
         TestingUtil.sleepThread(100);
      }

      assert cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE : "Expected 1, was " + cache.size();
      //let eviction manager kick in
      Util.sleep(3000);
      MockMarshalledValueInterceptor interceptor = (MockMarshalledValueInterceptor) TestingUtil.findInterceptor(cache, MarshalledValueInterceptor.class);
      assert !interceptor.marshalledValueCreated.get();
   }

   static class MockMarshalledValueInterceptor extends MarshalledValueInterceptor {
      AtomicBoolean marshalledValueCreated = new AtomicBoolean();

      MockMarshalledValueInterceptor(StreamingMarshaller marshaller) {
         inject(marshaller, new InternalEntryFactoryImpl(), null);
      }

      @Override
      protected MarshalledValue createMarshalledValue(Object toWrap, InvocationContext ctx) {
         marshalledValueCreated.set(true);
         return super.createMarshalledValue(toWrap, ctx);
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         // Reset value so that changes due to invocation can be asserted
         if (marshalledValueCreated.get()) marshalledValueCreated.set(false);
         return super.visitEvictCommand(ctx, command);
      }
   }

   public static class EvictionPojo implements Externalizable {
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
