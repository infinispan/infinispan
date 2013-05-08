/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ObjectDuplicator;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests implicit marshalled values
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "marshall.MarshalledValueTest")
public class MarshalledValueTest extends MultipleCacheManagersTest {   
   private static final Log log = LogFactory.getLog(MarshalledValueTest.class);
   private MarshalledValueListenerInterceptor mvli;
   String k = "key", v = "value";

   protected void createCacheManagers() throws Throwable {
      Cache cache1, cache2;
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC).fluent().storeAsBinary().build();

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
      Cache cache1;
      cache1 = cache(0, "replSync");
      cache(1, "replSync");
      InterceptorChain chain = TestingUtil.extractComponent(cache1, InterceptorChain.class);
      chain.removeInterceptor(MarshalledValueListenerInterceptor.class);
      mvli = new MarshalledValueListenerInterceptor();
      chain.addInterceptorAfter(mvli, MarshalledValueInterceptor.class);
   }
   
   @AfterClass
   protected void destroy() {
      super.destroy();
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
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
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
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      
      assert cache1.isEmpty();
      Pojo value = new Pojo();
      log.trace(TestingUtil.extractComponent(cache1, InterceptorChain.class).toString());
      cache1.put("key", value);
      assert cache1.containsKey("key");
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

   public void testReleaseObjectKeyReferences() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
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
   
   public void testKeySetValuesEntrySetCollectionReferences() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      
      Pojo key1 = new Pojo(1), value1 = new Pojo(11), key2 = new Pojo(2), value2 = new Pojo(22);
      String key3 = "3", value3 = "three"; 
      cache1.put(key1, value1);
      cache1.put(key2, value2);
      cache1.put(key3, value3);
      
      Set expKeys = new HashSet();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);
      
      Set expValues = new HashSet();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);
      
      Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set expValueEntries = ObjectDuplicator.duplicateSet(expValues);
      
      Set keys = cache2.keySet();
      for (Object key : keys) assert expKeys.remove(key);
      assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";
      
      Collection values = cache2.values();
      for (Object key : values) assert expValues.remove(key);
      assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";
      
      Set<Map.Entry> entries = cache2.entrySet();
      for (Map.Entry entry : entries) {
         assert expKeyEntries.remove(entry.getKey());
         assert expValueEntries.remove(entry.getValue());
      }
      assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
      assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
      
      Collection[] collections = new Collection[]{keys, values, entries};
      Object newObj = new Object();
      List newObjCol = new ArrayList();
      newObjCol.add(newObj);
      for (Collection col : collections) {
         try {
            col.add(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         try {
            col.addAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.clear();
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.remove(key1);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.removeAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
         
         try {
            col.retainAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
      
      for (Map.Entry entry : entries) {
         try {
            entry.setValue(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
   }

   public void testEqualsAndHashCode() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, true, extractCacheMarshaller(cache(0)));
      assertDeserialized(mv);
      int oldHashCode = mv.hashCode();

      mv.serialize();
      assertSerialized(mv);
      assert oldHashCode == mv.hashCode();

      MarshalledValue mv2 = new MarshalledValue(pojo, true, extractCacheMarshaller(cache(0)));
      assertSerialized(mv);
      assertDeserialized(mv2);

      assert mv2.hashCode() == oldHashCode;
      assert mv.equals(mv2);
   }

   public void testMarshallValueWithCustomReadObjectMethod() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      CustomReadObjectMethod obj = new CustomReadObjectMethod();
      cache1.put("ab-key", obj);
      assert cache2.get("ab-key").equals(obj);
      
      ObjectThatContainsACustomReadObjectMethod anotherObj = new ObjectThatContainsACustomReadObjectMethod();
      anotherObj.anObjectWithCustomReadObjectMethod = obj;
      cache1.put("cd-key", anotherObj);
      assert cache2.get("cd-key").equals(anotherObj);
   }

   /**
    * Run this as last method as it creates and stops cache loaders, which might affect other tests.
    */
   public void testCacheLoaders() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      tearDown();

      Configuration cacheCofig = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      cacheCofig.setUseLazyDeserialization(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      DummyInMemoryCacheStore.Cfg clc = new DummyInMemoryCacheStore.Cfg();
      clc.setStoreName(getClass().getSimpleName());
      clmc.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig) clc));
      cacheCofig.setCacheLoaderManagerConfig(clmc);

      defineConfigurationOnAllManagers("replSync2", cacheCofig);
      waitForClusterToForm("replSync2");
      cache1 = cache(0, "replSync2");
      cache2 = cache(1, "replSync2");

      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
      assertSerializationCounts(2, 0);

      cache2.get("key");

      assertSerializationCounts(2, 1);
   }

   public void testCallbackValues() throws Exception {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
      MockListener l = new MockListener();
      cache1.addListener(l);
      try {
         Pojo pojo = new Pojo();
         cache1.put("key", pojo);
         assert l.newValue instanceof Pojo : "recieved " + l.newValue.getClass().getName();
         // +1 due to new marshallable checks
         assertSerializationCounts(2, 0);
      } finally {
         cache1.removeListener(l);
      }
   }

   public void testRemoteCallbackValues() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      MockListener l = new MockListener();
      cache2.addListener(l);
      try {
         Pojo pojo = new Pojo();
         // Mock listener will force deserialization on transport thread. Ignore this by setting b to false.
         pojo.b = false;
         cache1.put("key", pojo);
         assert l.newValue instanceof Pojo;
         assertSerializationCounts(1, 1);
      } finally {
         cache2.removeListener(l);
      }
   }
   
   public void testEvictWithMarshalledValueKey() {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
      Pojo pojo = new Pojo();
      cache1.put(pojo, pojo);
      cache1.evict(pojo);
      assert !cache1.containsKey(pojo);
   }

   public void testModificationsOnSameCustomKey() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      Pojo key = new Pojo();

      cache1.put(key, "1");
      cache2.put(key, "2");

      // Deserialization only occurs when the cache2.put occurs, not during transport thread execution.
      // Serialized 4 times:
      //    twice when put is invoked on cache1, one to check if the type can be serialized, 2nd to replicate.
      //    twice when put is invoked on cache2, one to check if the type can be serialized, 2nd to replicate.
      assertSerializationCounts(4, 1);
   }

   public void testReturnValueDeserialization() { 
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Pojo v1 = new Pojo(1);
      cache1.put("1", v1);
      Pojo previous = (Pojo) cache1.put("1", new Pojo(2));
      assert previous.equals(v1);
   }

   public void testEquality() {
      byte[] prevBytes = TestingUtil.generateRandomString(1310)
            .getBytes(Charset.forName("UTF-8"));

      String value = "galder";
      MarshalledValue mv = new MarshalledValue(
            value, true, extractCacheMarshaller(cache(0, "replSync")));
      MarshalledValue mv2 = new MarshalledValue(
            value, false, extractCacheMarshaller(cache(0, "replSync")));

      // Simulate that the marshalled value had a bigger value before
      mv2.instance = prevBytes;
      mv2.serialize();
      // Now back to the original instance and force it to serialize again
      mv2.instance = value;
      mv2.raw = null;
      mv2.serialize();
      mv2.instance = null;

      assertEquals(mv, mv2);
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
      public int i;
      boolean b = true;
      static int serializationCount, deserializationCount;
      final Log log = LogFactory.getLog(Pojo.class);
      private static final long serialVersionUID = -2888014339659501395L;

      public Pojo(int i) {
         this.i = i;
      }

      public Pojo() {
      }

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
         int serCount = updateSerializationCount();
         log.trace("serializationCount=" + serCount);
      }

      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         if (b) {
            // TODO: Find a better way to make sure a transport (JGroups) thread is not attempting to deserialize stuff
            assert !Thread.currentThread().getName().startsWith("OOB") : "Transport (JGroups) thread is trying to deserialize stuff!!";
         }
         int deserCount = updateDeserializationCount();
         log.trace("deserializationCount=" + deserCount);
      }

      public int updateSerializationCount() {
         return ++serializationCount;
      }

      public int updateDeserializationCount() {
         return ++deserializationCount;
      }
   }

   public static class ObjectThatContainsACustomReadObjectMethod implements Serializable {
      private static final long serialVersionUID = 1L;
//      Integer id;
      public CustomReadObjectMethod anObjectWithCustomReadObjectMethod;
      Integer balance;
//      String branch;
      
      public boolean equals(Object obj) {
         if (obj == this)
            return true;
         if (!(obj instanceof ObjectThatContainsACustomReadObjectMethod))
            return false;
         ObjectThatContainsACustomReadObjectMethod acct = (ObjectThatContainsACustomReadObjectMethod) obj;
//         if (!safeEquals(id, acct.id))
//            return false;
//         if (!safeEquals(branch, acct.branch))
//            return false;
         if (!safeEquals(balance, acct.balance))
            return false;
         if (!safeEquals(anObjectWithCustomReadObjectMethod, acct.anObjectWithCustomReadObjectMethod))
            return false;
         return true;
      }

      public int hashCode() {
         int result = 17;
//         result = result * 31 + safeHashCode(id);
//         result = result * 31 + safeHashCode(branch);
         result = result * 31 + safeHashCode(balance);
         result = result * 31 + safeHashCode(anObjectWithCustomReadObjectMethod);
         return result;
      }
      
      private static int safeHashCode(Object obj) {
         return obj == null ? 0 : obj.hashCode();
      }

      private static boolean safeEquals(Object a, Object b) {
         return (a == b || (a != null && a.equals(b)));
      }
   }

   public static class CustomReadObjectMethod implements Serializable {
      private static final long serialVersionUID = 1L;
      String lastName;
      String ssn;
      transient boolean deserialized;

      public CustomReadObjectMethod( ) {
         this("Zamarreno", "234-567-8901");
      }

      public CustomReadObjectMethod(String lastName, String ssn) {
         this.lastName = lastName;
         this.ssn = ssn;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj == this) return true;
         if (!(obj instanceof CustomReadObjectMethod)) return false;
         CustomReadObjectMethod pk = (CustomReadObjectMethod) obj;
         if (!lastName.equals(pk.lastName)) return false;
         if (!ssn.equals(pk.ssn)) return false;
         return true;
      }

      @Override
      public int hashCode( ) {
         int result = 17;
         result = result * 31 + lastName.hashCode();
         result = result * 31 + ssn.hashCode();
         return result;
      }

      private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
         ois.defaultReadObject();
         deserialized = true;
      }
   }
}
