package org.infinispan.all.embedded;

import org.infinispan.Cache;
import org.infinispan.all.embedded.util.EmbeddedUtils;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.Serializable;

import static org.junit.Assert.*;

/**
 * Tests implicit marshalled values in the scope of infinispan-embedded uber-jar
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Tomas Sykora (tsykora@redhat.com)
 * @since 4.0
 */

public class EmbeddedAllMarshalledValueTest {
   private static final Log log = LogFactory.getLog(EmbeddedAllMarshalledValueTest.class);
   private MarshalledValueListenerInterceptor mvli;

   private static EmbeddedCacheManager manager;
   private static EmbeddedCacheManager manager2;
   private static Cache<Object, Object> cache1;
   private static Cache<Object, Object> cache2;

   @BeforeClass
   public static void beforeTest() throws Exception {

      GlobalConfiguration globalConfiguration = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node1")
            .build();

      GlobalConfiguration globalConfiguration2 = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node2")
            .build();

      manager = new DefaultCacheManager(globalConfiguration);
      manager2 = new DefaultCacheManager(globalConfiguration2);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true)
            .dataContainer().storeAsBinary().enable();

      manager.defineConfiguration("replicated-cache", builder.build());
      manager2.defineConfiguration("replicated-cache", builder.build());

      cache1 = manager.getCache("replicated-cache");
      cache2 = manager2.getCache("replicated-cache");
   }

   @AfterClass
   public static void cleanUp() {
      EmbeddedUtils.killCacheManagers(true, manager, manager2);
   }

   @Before
   public void addInterceptor() {
      InterceptorChain chain = EmbeddedUtils.extractComponent(cache1, InterceptorChain.class);
      chain.removeInterceptor(MarshalledValueListenerInterceptor.class);
      mvli = new MarshalledValueListenerInterceptor();
      chain.addInterceptorAfter(mvli, MarshalledValueInterceptor.class);
   }

   @Before
   public void resetSerializationCounts() {
      cache1.clear();
      cache2.clear();
      Pojo.serializationCount = 0;
      Pojo.deserializationCount = 0;
   }

   @Test
   public void testPresenceOfMarshalledValueInterceptor() throws Throwable {

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);

      // Prime the IsMarshallableInterceptor so that it doesn't trigger additional serialization during tests
      Pojo key = new Pojo(-1);
      cache1.get(key);
      assertSerializationCounts(1, 0);
      cache2.get(key);
      assertSerializationCounts(2, 0);
   }

   private void assertMarshalledValueInterceptorPresent(Cache c) {
      InterceptorChain ic1 = EmbeddedUtils.extractComponent(c, InterceptorChain.class);
      assertTrue(ic1.containsInterceptorType(MarshalledValueInterceptor.class));
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assertEquals("Serialization count mismatch", serializationCount, Pojo.serializationCount);
      assertEquals("Deserialization count mismatch", deserializationCount, Pojo.deserializationCount);
   }

   private void assertSerialized(MarshalledValue mv) {
      assertTrue("Should be serialized", mv.getRaw() != null);
   }

   @Test
   public void testNonSerializable() {

      try {
         cache1.put("Hello", new Object());
         fail("Should have failed");
      } catch (CacheException expected) {
      }

      assertTrue("Call should not have gone beyond the MarshalledValueInterceptor", mvli.invocationCount == 0);

      try {
         cache1.put(new Object(), "Hello");
         fail("Should have failed");
      } catch (CacheException expected) {
      }

      assertTrue("Call should not have gone beyond the MarshalledValueInterceptor", mvli.invocationCount == 0);
   }

   @Test
   public void testReleaseObjectValueReferences() {

      assertTrue(cache1.isEmpty());
      Pojo value = new Pojo();
      log.trace(EmbeddedUtils.extractComponent(cache1, InterceptorChain.class).toString());
      cache1.put("key", value);
      assertTrue(cache1.containsKey("key"));
      assertSerializationCounts(1, 1);

      DataContainer dc1 = EmbeddedUtils.extractComponent(cache1, DataContainer.class);

      InternalCacheEntry ice = dc1.get("key");
      Object o = ice.getValue();
      assertTrue(o instanceof MarshalledValue);
      MarshalledValue mv = (MarshalledValue) o;
      assertEquals(value, cache1.get("key"));
      assertSerializationCounts(1, 2);
      assertSerialized(mv);

      // now on cache 2
      DataContainer dc2 = EmbeddedUtils.extractComponent(cache2, DataContainer.class);
      ice = dc2.get("key");
      o = ice.getValue();
      assertTrue(o instanceof MarshalledValue);
      mv = (MarshalledValue) o;
      assertSerialized(mv); // this proves that unmarshalling on the recipient cache instance is lazy

      assertEquals(value, cache2.get("key"));
      assertSerializationCounts(1, 3);
      assertSerialized(mv);
   }

   @Test
   public void testEqualsAndHashCode() throws Exception {
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, EmbeddedUtils.extractCacheMarshaller(cache1));
      int oldHashCode = mv.hashCode();
      assertSerialized(mv);
      assertTrue(oldHashCode == mv.hashCode());

      MarshalledValue mv2 = new MarshalledValue(pojo, EmbeddedUtils.extractCacheMarshaller(cache1));
      assertSerialized(mv);

      assertTrue(mv2.hashCode() == oldHashCode);
      assertEquals(mv, mv2);
   }

   @Test
   public void testMarshallValueWithCustomReadObjectMethod() {
      CustomReadObjectMethod obj = new CustomReadObjectMethod();
      cache1.put("ab-key", obj);
      assertEquals(obj, cache2.get("ab-key"));

      ObjectThatContainsACustomReadObjectMethod anotherObj = new ObjectThatContainsACustomReadObjectMethod();
      anotherObj.anObjectWithCustomReadObjectMethod = obj;
      cache1.put("cd-key", anotherObj);
      assertEquals(anotherObj, cache2.get("cd-key"));
   }

   @Test
   public void testModificationsOnSameCustomKey() {

      Pojo key1 = new Pojo();
      log.trace("First put");
      cache1.put(key1, "1");
      // 1 serialization on cache1 (the primary), when replicating the command to cache2 (the backup)
      assertSerializationCounts(1, 0);

      log.trace("Second put");
      Pojo key2 = new Pojo();
      cache2.put(key2, "2");
      // 1 serialization on cache2 for key2, when replicating the command to cache1 (the primary)
      // 1 serialization on cache1 for key1
      assertSerializationCounts(2, 0);
   }

   @Test
   public void testReturnValueDeserialization() {
      Pojo v1 = new Pojo(1);
      cache1.put("1", v1);
      Pojo previous = (Pojo) cache1.put("1", new Pojo(2));
      assertEquals(v1, previous);
   }

   class MarshalledValueListenerInterceptor extends CommandInterceptor {
      int invocationCount = 0;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         invocationCount++;
         Object retval = invokeNextInterceptor(ctx, command);
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

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) return false;
         if (i != pojo.i) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         int serCount = updateSerializationCount();
         log.trace("serializationCount=" + serCount);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         if (b) {
            // TODO: Find a better way to make sure a transport (JGroups) thread is not attempting to deserialize stuff
            assertTrue("Transport (JGroups) thread is trying to deserialize stuff!!", !Thread.currentThread().getName().startsWith("OOB"));
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
      public CustomReadObjectMethod anObjectWithCustomReadObjectMethod;
      Integer balance;

      @Override
      public boolean equals(Object obj) {
         if (obj == this)
            return true;
         if (!(obj instanceof ObjectThatContainsACustomReadObjectMethod))
            return false;
         ObjectThatContainsACustomReadObjectMethod acct = (ObjectThatContainsACustomReadObjectMethod) obj;
         if (!safeEquals(balance, acct.balance))
            return false;
         if (!safeEquals(anObjectWithCustomReadObjectMethod, acct.anObjectWithCustomReadObjectMethod))
            return false;
         return true;
      }

      @Override
      public int hashCode() {
         int result = 17;
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

      public CustomReadObjectMethod() {
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
      public int hashCode() {
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
