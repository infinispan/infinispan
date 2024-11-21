package org.infinispan.client.hotrod.tx.util;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.lang.reflect.Method;

/**
 * A key and value generator for Hot Rod testing.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface KeyValueGenerator<K, V> {

   KeyValueGenerator<String, String> STRING_GENERATOR = new KeyValueGenerator<String, String>() {
      @Override
      public String generateKey(Method method, int index) {
         return k(method, index);
      }

      @Override
      public String generateValue(Method method, int index) {
         return v(method, index);
      }

      @Override
      public void assertValueEquals(String expected, String actual) {
         assertEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "STRING";
      }
   };

   KeyValueGenerator<byte[], byte[]> BYTE_ARRAY_GENERATOR = new KeyValueGenerator<byte[], byte[]>() {

      @Override
      public byte[] generateKey(Method method, int index) {
         return k(method, index).getBytes();
      }

      @Override
      public byte[] generateValue(Method method, int index) {
         return v(method, index).getBytes();
      }

      @Override
      public void assertValueEquals(byte[] expected, byte[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "BYTE_ARRAY";
      }
   };

   KeyValueGenerator<Object[], Object[]> GENERIC_ARRAY_GENERATOR = new KeyValueGenerator<Object[], Object[]>() {
      @Override
      public Object[] generateKey(Method method, int index) {
         return new Object[]{method.getName(), "key", index};
      }

      @Override
      public Object[] generateValue(Method method, int index) {
         return new Object[]{method.getName(), "value", index};
      }

      @Override
      public void assertValueEquals(Object[] expected, Object[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "GENERIC_ARRAY";
      }
   };

   K generateKey(Method method, int index);

   V generateValue(Method method, int index);

   void assertValueEquals(V expected, V actual);

}
