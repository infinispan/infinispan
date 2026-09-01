package org.infinispan.rest.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "rest.stream.CacheChunkedStreamTest")
public class CacheChunkedStreamTest {

   public void testReadContentAsBytesWithNull() {
      byte[] result = assertDoesNotThrow(() -> CacheChunkedStream.readContentAsBytes(null));
      assertArrayEquals(new byte[0], result);
   }

   public void testReadContentAsBytesWithByteArray() {
      byte[] input = {1, 2, 3};
      byte[] result = CacheChunkedStream.readContentAsBytes(input);
      assertArrayEquals(input, result);
   }

   public void testReadContentAsBytesWithWrappedByteArray() {
      byte[] bytes = {4, 5, 6};
      WrappedByteArray wrapped = new WrappedByteArray(bytes);
      byte[] result = CacheChunkedStream.readContentAsBytes(wrapped);
      assertArrayEquals(bytes, result);
   }

   public void testReadContentAsBytesWithString() {
      byte[] result = CacheChunkedStream.readContentAsBytes("hello");
      assertArrayEquals("hello".getBytes(), result);
   }
}
