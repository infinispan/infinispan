package org.infinispan.server.functional.hotrod;

import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.test.TestingUtil;
import org.junit.jupiter.api.Assertions;

/**
 * A key and value generator for Hot Rod testing.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface KeyValueGenerator<K, V> {

   KeyValueGenerator<String, String> STRING_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public String key(int index) {
         return TestingUtil.k(CallerId.getCallerMethodName(3), index);
      }

      @Override
      public String value(int index) {
         return v(CallerId.getCallerMethodName(3), index);
      }

      @Override
      public void assertEquals(String expected, String actual) {
         Assertions.assertEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "STRING";
      }
   };

   KeyValueGenerator<byte[], byte[]> BYTE_ARRAY_GENERATOR = new KeyValueGenerator<>() {

      @Override
      public byte[] key(int index) {
         return TestingUtil.k(CallerId.getCallerMethodName(3), index).getBytes();
      }

      @Override
      public byte[] value(int index) {
         return v(CallerId.getCallerMethodName(3), index).getBytes();
      }

      @Override
      public void assertEquals(byte[] expected, byte[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "BYTE_ARRAY";
      }
   };

   KeyValueGenerator<Object[], Object[]> GENERIC_ARRAY_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public Object[] key(int index) {
         return new Object[]{CallerId.getCallerMethodName(3), "key", index};
      }

      @Override
      public Object[] value(int index) {
         return new Object[]{CallerId.getCallerMethodName(3), "value", index};
      }

      @Override
      public void assertEquals(Object[] expected, Object[] actual) {
         assertArrayEquals(expected, actual);
      }

      @Override
      public String toString() {
         return "GENERIC_ARRAY";
      }
   };

   K key(int index);

   V value(int index);

   void assertEquals(V expected, V actual);

}
