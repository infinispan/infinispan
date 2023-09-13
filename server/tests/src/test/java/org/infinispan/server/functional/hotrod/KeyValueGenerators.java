package org.infinispan.server.functional.hotrod;

import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.test.TestingUtil;
import org.junit.jupiter.api.Assertions;

public class KeyValueGenerators {

   private KeyValueGenerators() {}
   static String getCallerMethodName(int index) {
      return CallerId.getCallerMethodName(index);
   }

   public static final KeyValueGenerator<String, String> STRING_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public String key(int index) {
         return TestingUtil.k(getCallerMethodName(4), index);
      }

      @Override
      public String value(int index) {
         return v(getCallerMethodName(4), index);
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

   public static final KeyValueGenerator<byte[], byte[]> BYTE_ARRAY_GENERATOR = new KeyValueGenerator<>() {

      @Override
      public byte[] key(int index) {
         return TestingUtil.k(getCallerMethodName(4), index).getBytes();
      }

      @Override
      public byte[] value(int index) {
         return v(getCallerMethodName(4), index).getBytes();
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

   public static final KeyValueGenerator<Object[], Object[]> GENERIC_ARRAY_GENERATOR = new KeyValueGenerator<>() {
      @Override
      public Object[] key(int index) {
         return new Object[]{getCallerMethodName(4), "key", index};
      }

      @Override
      public Object[] value(int index) {
         return new Object[]{getCallerMethodName(4), "value", index};
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
}
