package org.infinispan.jboss.marshalling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.jboss.marshalling.commons.CheckedClassResolver;
import org.infinispan.test.AbstractInfinispanTest;
import org.jboss.marshalling.ByteInput;
import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.jboss.CheckedClassResolverTest")
public class CheckedClassResolverTest extends AbstractInfinispanTest {

   public void testAllowedClassDeserializes() throws Exception {
      ClassAllowList allowList = new ClassAllowList(List.of("org\\.infinispan\\.jboss\\.marshalling\\..*"));
      AllowedPojo original = new AllowedPojo("hello");

      byte[] bytes = marshall(original, allowList);
      AllowedPojo result = (AllowedPojo) unmarshall(bytes, allowList);
      assertEquals(original.value, result.value);
   }

   public void testBlockedClassThrows() throws Exception {
      ClassAllowList writeList = new ClassAllowList(List.of("org\\.infinispan\\.jboss\\.marshalling\\..*"));
      ClassAllowList readList = new ClassAllowList();
      AllowedPojo original = new AllowedPojo("blocked");

      byte[] bytes = marshall(original, writeList);
      CacheException ex = assertThrows(CacheException.class, () -> unmarshall(bytes, readList));
      assertNotNull(ex.getMessage());
      assert ex.getMessage().contains("AllowedPojo") : "Expected message to mention the blocked class";
   }

   public void testBlockedProxyClassThrows() throws Exception {
      ClassAllowList writeList = new ClassAllowList(List.of(".*"));
      ClassAllowList readList = new ClassAllowList(List.of("org\\.infinispan\\.jboss\\.marshalling\\..*"));

      Object proxy = Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] { Runnable.class, Serializable.class },
            new NoOpHandler());
      byte[] bytes = marshall(proxy, writeList);
      CacheException ex = assertThrows(CacheException.class, () -> unmarshall(bytes, readList));
      assertNotNull(ex.getMessage());
      assert ex.getMessage().contains("Runnable") : "Expected message to mention the blocked proxy interface";
   }

   public void testAllowedProxyClassDeserializes() throws Exception {
      ClassAllowList allowList = new ClassAllowList(
            List.of(".*"));

      Object proxy = Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] { Runnable.class, Serializable.class },
            new NoOpHandler());
      byte[] bytes = marshall(proxy, allowList);
      Object result = unmarshall(bytes, allowList);
      assertNotNull(result);
      assert Proxy.isProxyClass(result.getClass());
   }

   private byte[] marshall(Object obj, ClassAllowList allowList) throws Exception {
      MarshallerFactory factory = newFactory();
      MarshallingConfiguration config = new MarshallingConfiguration();
      config.setClassResolver(new CheckedClassResolver(allowList, getClass().getClassLoader()));

      Marshaller marshaller = factory.createMarshaller(config);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
      ByteOutput byteOutput = Marshalling.createByteOutput(baos);
      marshaller.start(byteOutput);
      try {
         marshaller.writeObject(obj);
      } finally {
         marshaller.finish();
      }
      return baos.toByteArray();
   }

   private Object unmarshall(byte[] bytes, ClassAllowList allowList) throws Exception {
      MarshallerFactory factory = newFactory();
      MarshallingConfiguration config = new MarshallingConfiguration();
      config.setClassResolver(new CheckedClassResolver(allowList, getClass().getClassLoader()));

      Unmarshaller unmarshaller = factory.createUnmarshaller(config);
      ByteInput byteInput = Marshalling.createByteInput(new ByteArrayInputStream(bytes));
      unmarshaller.start(byteInput);
      try {
         return unmarshaller.readObject();
      } finally {
         unmarshaller.finish();
      }
   }

   private static MarshallerFactory newFactory() throws Exception {
      return (MarshallerFactory) Thread.currentThread().getContextClassLoader()
            .loadClass("org.jboss.marshalling.river.RiverMarshallerFactory")
            .getDeclaredConstructor().newInstance();
   }

   public static class AllowedPojo implements Serializable {
      private static final long serialVersionUID = 1L;
      String value;

      public AllowedPojo() {
      }

      public AllowedPojo(String value) {
         this.value = value;
      }
   }

   static class NoOpHandler implements InvocationHandler, Serializable {
      private static final long serialVersionUID = 1L;

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) {
         return null;
      }
   }
}
