package org.horizon.marshall;

import org.horizon.AdvancedCache;
import org.horizon.Cache;
import org.horizon.CacheException;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.container.DataContainer;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.context.InvocationContext;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.interceptors.MarshalledValueInterceptor;
import org.horizon.interceptors.base.CommandInterceptor;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.dummy.DummyInMemoryCacheStore;
import org.horizon.notifications.Listener;
import org.horizon.notifications.cachelistener.annotation.CacheEntryModified;
import org.horizon.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;

/**
 * Tests implicit marshalled values
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "marshall.MarshalledValueTest")
public class MarshalledValueTest extends MultipleCacheManagersTest {
   private Cache cache1, cache2;
   private MarshalledValueListenerInterceptor mvli;
   String k = "key", v = "value";

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      replSync.setUseLazyDeserialization(true);

      createClusteredCaches(2, "replSync", replSync);

      cache1 = cache(0, "replSync");
      cache2 = cache(1, "replSync");

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
   }

   private void assertMarshalledValueInterceptorPresent(Cache c) {
      InterceptorChain ic1 = TestingUtil.extractComponent(c, InterceptorChain.class);
      assert ic1.containsInterceptorType(MarshalledValueInterceptor.class);
   }

   @BeforeMethod
   public void addMarshalledValueInterceptor() {
      mvli = new MarshalledValueListenerInterceptor();
      ((AdvancedCache) cache1).addInterceptorAfter(mvli, MarshalledValueInterceptor.class);
   }

   @AfterMethod
   public void tearDown() {
      Pojo.serializationCount = 0;
      Pojo.deserializationCount = 0;
   }

   private void assertOnlyOneRepresentationExists(MarshalledValue mv) {
      assert (mv.instance != null && mv.raw == null) || (mv.instance == null && mv.raw != null) : "Only instance or raw representations should exist in a MarshalledValue; never both";
   }

   private void assertSerialized(MarshalledValue mv) {
      assert mv.raw != null : "Should be serialized";
   }

   private void assertDeserialized(MarshalledValue mv) {
      assert mv.instance != null : "Should be deserialized";
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assert Pojo.serializationCount == serializationCount : "Serialization count: expected " + serializationCount + " but was " + Pojo.serializationCount;
      assert Pojo.deserializationCount == deserializationCount : "Deserialization count: expected " + deserializationCount + " but was " + Pojo.deserializationCount;
   }

   public void testNonSerializable() {
      try {
         cache1.put("Hello", new Object());
         assert false : "Should have failed";
      }
      catch (CacheException expected) {

      }

      assert mvli.invocationCount == 0 : "Call should not have gone beyond the MarshalledValueInterceptor";

      try {
         cache1.put(new Object(), "Hello");
         assert false : "Should have failed";
      }
      catch (CacheException expected) {

      }

      assert mvli.invocationCount == 0 : "Call should not have gone beyond the MarshalledValueInterceptor";
   }

   public void testReleaseObjectValueReferences() {
      Pojo value = new Pojo();
      cache1.put("key", value);
      assertSerializationCounts(1, 0);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      InternalCacheEntry ice = dc1.get("key");
      Object o = ice.getValue();
      assert o instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) o;
      assertDeserialized(mv);
      assert cache1.get("key").equals(value);
      assertDeserialized(mv);
      assertSerializationCounts(1, 0);
      cache1.compact();
      assertSerializationCounts(2, 0);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);

      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      ice = dc2.get("key");
      o = ice.getValue();
      assert o instanceof MarshalledValue;
      mv = (MarshalledValue) o;
      assertSerialized(mv); // this proves that unmarshalling on the recipient cache instance is lazy

      assert cache2.get("key").equals(value);
      assertDeserialized(mv);
      assertSerializationCounts(2, 1);
      cache2.compact();
      assertSerializationCounts(2, 1);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);
   }

   public void testReleaseObjectKeyReferences() throws IOException, ClassNotFoundException {
      Pojo key = new Pojo();
      cache1.put(key, "value");

      assertSerializationCounts(1, 0);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      Object o = dc1.keySet().iterator().next();
      assert o instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) o;
      assertDeserialized(mv);

      assert cache1.get(key).equals("value");
      assertDeserialized(mv);
      assertSerializationCounts(1, 0);

      cache1.compact();
      assertSerializationCounts(2, 0);
      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);


      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      o = dc2.keySet().iterator().next();
      assert o instanceof MarshalledValue;
      mv = (MarshalledValue) o;
      assertSerialized(mv);
      assert cache2.get(key).equals("value");
      assertSerializationCounts(2, 1);
      assertDeserialized(mv);
      cache2.compact();

      assertOnlyOneRepresentationExists(mv);
      assertSerialized(mv);
      assertSerializationCounts(2, 1);
   }

   public void testEqualsAndHashCode() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, true);
      assertDeserialized(mv);
      int oldHashCode = mv.hashCode();

      mv.serialize();
      assertSerialized(mv);
      assert oldHashCode == mv.hashCode();

      MarshalledValue mv2 = new MarshalledValue(pojo, true);
      assertSerialized(mv);
      assertDeserialized(mv2);

      assert mv2.hashCode() == oldHashCode;
      assert mv.equals(mv2);
   }

   public void assertUseOfMagicNumbers() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, true);


      HorizonMarshaller marshaller = new HorizonMarshaller();

      // start the test
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bout);
      marshaller.objectToObjectStream(mv, out);
      out.close();
      bout.close();

      // check that the rest just contains a byte stream which a MarshalledValue will be able to deserialize.
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      ObjectInputStream in = new ObjectInputStream(bin);

      assert in.read() == HorizonMarshaller.MAGICNUMBER_MARSHALLEDVALUE;
      MarshalledValue recreated = new MarshalledValue();
      recreated.readExternal(in);

      // there should be nothing more
      assert in.available() == 0;
      in.close();
      bin.close();

      assertSerialized(recreated);
      assert recreated.equals(mv);

      // since both objects being compared are serialized, the equals() above should just compare byte arrays.
      assertSerialized(recreated);
      assertOnlyOneRepresentationExists(recreated);
   }

   /**
    * Run this as last method as it creates and stops cache loaders, which might affect other tests.
    */
   @Test(dependsOnMethods = "org.horizon.marshall.MarshalledValueTest.test(?!CacheLoaders)[a-zA-Z]*")
   public void testCacheLoaders() throws CloneNotSupportedException {
      tearDown();

      Configuration cacheCofig = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      cacheCofig.setUseLazyDeserialization(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      DummyInMemoryCacheStore.Cfg clc = new DummyInMemoryCacheStore.Cfg();
      clc.setStore(getClass().getSimpleName());
      clmc.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig) clc));
      cacheCofig.setCacheLoaderManagerConfig(clmc);

      defineCacheOnAllManagers("replSync2", cacheCofig);
      cache1 = cache(0, "replSync2");
      cache2 = cache(1, "replSync2");

      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
      assertSerializationCounts(1, 0);

      cache2.get("key");

      assertSerializationCounts(1, 1);
   }

   public void testCallbackValues() {
      MockListener l = new MockListener();
      cache1.addListener(l);
      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assert l.newValue != null;
      assert l.newValue instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) l.newValue;
      assert mv.instance instanceof Pojo;
      assertSerializationCounts(1, 0);
   }

   public void testRemoteCallbackValues() throws Exception {
      MockListener l = new MockListener();
      cache2.addListener(l);
      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assert l.newValue != null;
      assert l.newValue instanceof MarshalledValue;
      MarshalledValue mv = (MarshalledValue) l.newValue;
      assert mv.get() instanceof Pojo;
      assertSerializationCounts(1, 1);
   }

   @Listener
   public static class MockListener {
      Object newValue;

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent e) {
         if (!e.isPre()) newValue = e.getValue();
      }
   }

   class MarshalledValueListenerInterceptor extends CommandInterceptor {
      int invocationCount = 0;

      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         invocationCount++;
         if (command.getKey() instanceof MarshalledValue)
            assertOnlyOneRepresentationExists((MarshalledValue) command.getKey());
         if (command.getValue() instanceof MarshalledValue)
            assertOnlyOneRepresentationExists((MarshalledValue) command.getValue());
         Object retval = invokeNextInterceptor(ctx, command);
         if (retval instanceof MarshalledValue) assertOnlyOneRepresentationExists((MarshalledValue) retval);
         return retval;
      }

   }

   public static class Pojo implements Externalizable {
      int i;
      boolean b;
      static int serializationCount, deserializationCount;

      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) return false;
         if (i != pojo.i) return false;

         return true;
      }

      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         serializationCount++;
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         deserializationCount++;
      }
   }
}
