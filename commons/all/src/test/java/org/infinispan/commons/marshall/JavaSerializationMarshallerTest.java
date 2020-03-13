package org.infinispan.commons.marshall;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

import org.infinispan.commons.CacheException;
import org.junit.Assert;
import org.junit.Test;

public class JavaSerializationMarshallerTest {

   @Test
   public void testPrimitiveArrays() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();

      byte[] bytes = marshaller.objectToByteBuffer(new byte[0]);
      assertArrayEquals(new byte[0], (byte[]) marshaller.objectFromByteBuffer(bytes));

      bytes = marshaller.objectToByteBuffer(new short[0]);
      assertArrayEquals(new short[0], (short[]) marshaller.objectFromByteBuffer(bytes));

      bytes = marshaller.objectToByteBuffer(new int[0]);
      assertArrayEquals(new int[0], (int[]) marshaller.objectFromByteBuffer(bytes));

      bytes = marshaller.objectToByteBuffer(new long[0]);
      assertArrayEquals(new long[0], (long[]) marshaller.objectFromByteBuffer(bytes));

      bytes = marshaller.objectToByteBuffer(new float[0]);
      assertArrayEquals(new float[0], (float[]) marshaller.objectFromByteBuffer(bytes), 0);

      bytes = marshaller.objectToByteBuffer(new double[0]);
      assertArrayEquals(new double[0], (double[]) marshaller.objectFromByteBuffer(bytes), 0);

      bytes = marshaller.objectToByteBuffer(new char[0]);
      assertArrayEquals(new char[0], (char[]) marshaller.objectFromByteBuffer(bytes));

      bytes = marshaller.objectToByteBuffer(new boolean[0]);
      assertArrayEquals(new boolean[0], (boolean[]) marshaller.objectFromByteBuffer(bytes));
   }

   @Test
   public void testBoxedPrimitivesAndArray() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
      isMarshallable(marshaller, Byte.MAX_VALUE);
      isMarshallable(marshaller, Short.MAX_VALUE);
      isMarshallable(marshaller, Integer.MAX_VALUE);
      isMarshallable(marshaller, Long.MAX_VALUE);
      isMarshallable(marshaller, Float.MAX_VALUE);
      isMarshallable(marshaller, Double.MAX_VALUE);
      isMarshallable(marshaller, 'c');
      isMarshallable(marshaller, "String");
   }

   @Test
   public void testMath() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
      isMarshallable(marshaller, BigDecimal.TEN);
      isMarshallable(marshaller, BigInteger.TEN);
   }

   @Test
   public void testDate() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
      isMarshallable(marshaller, Instant.now());
   }

   @Test
   public void testCustomClassAndArray() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
      byte[] bytes = marshaller.objectToByteBuffer(new CustomClass());
      try {
         marshaller.objectFromByteBuffer(bytes);
         Assert.fail("Expected an exception to be thrown when reading the Serialization bytes");
      } catch (CacheException e) {
         assertTrue(e.getMessage().contains("blocked by deserialization white list"));
      }

      marshaller.whiteList.addClasses(CustomClass.class);

      assertNotNull(marshaller.objectFromByteBuffer(bytes));
      isMarshallable(marshaller, new CustomClass());
   }

   private static <V> void isMarshallable(Marshaller marshaller, V o) throws Exception {
      byte[] bytes = marshaller.objectToByteBuffer(o);
      assertEquals(o, marshaller.objectFromByteBuffer(bytes));

      V[] array = (V[]) Array.newInstance(o.getClass(), 1);
      bytes = marshaller.objectToByteBuffer(array);
      assertArrayEquals(array, (V[]) marshaller.objectFromByteBuffer(bytes));
   }

   private static class CustomClass implements Serializable {
      @Override
      public int hashCode() {
         return 0;
      }

      @Override
      public boolean equals(Object obj) {
         return obj instanceof CustomClass;
      }
   }
}
