package org.infinispan.hotrod.test;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.opentest4j.AssertionFailedError;

/**
 * A key and value generator for Hot Rod testing.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface KeyValueGenerator<K, V> {

   KeyValueGenerator<String, String> STRING_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public String generateKey(String method, int index) {
         return k(method, index);
      }

      @Override
      public String generateValue(String method, int index) {
         return v(method, index);
      }

      @Override
      public void assertValueEquals(String expected, String actual) {
         assertEquals(expected, actual);
      }

      @Override
      public void assertKeyEquals(String expected, String actual) {
         assertEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "STRING";
      }
   };

   KeyValueGenerator<byte[], byte[]> BYTE_ARRAY_GENERATOR = new KeyValueGenerator<>() {

      @Override
      public byte[] generateKey(String method, int index) {
         return k(method, index).getBytes();
      }

      @Override
      public byte[] generateValue(String method, int index) {
         return v(method, index).getBytes();
      }

      @Override
      public void assertValueEquals(byte[] expected, byte[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public void assertKeyEquals(byte[] expected, byte[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "BYTE_ARRAY";
      }
   };

   KeyValueGenerator<Object[], Object[]> GENERIC_ARRAY_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public Object[] generateKey(String method, int index) {
         return new Object[] { method, "key", index };
      }

      @Override
      public Object[] generateValue(String method, int index) {
         return new Object[] { method, "value", index };
      }

      @Override
      public void assertValueEquals(Object[] expected, Object[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public void assertKeyEquals(Object[] expected, Object[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "GENERIC_ARRAY";
      }
   };

   K generateKey(String method, int index);

   V generateValue(String method, int index);

   void assertValueEquals(V expected, V actual);

   void assertKeyEquals(K expected, K actual);

   default void assertValueNotEquals(V expected, V actual) {
      try {
         assertValueEquals(expected, actual);
      } catch (AssertionFailedError ignore) {
         return;
      }
      fail("Value should be different");
   }
}
