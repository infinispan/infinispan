package org.infinispan.commons.marshall;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

/**
 * Verifies that marshallers read an object from the requested range of a byte array, ignoring the bytes around it.
 */
public class MarshallerByteRangeTest {

   private static final int PADDING = 3;

   @Test
   public void testIdentityMarshallerReturnsOnlyTheRange() throws Exception {
      byte[] content = {1, 2, 3};

      Object read = IdentityMarshaller.INSTANCE.objectFromByteBuffer(pad(content), PADDING, content.length);

      assertArrayEquals(content, (byte[]) read);
   }

   @Test
   public void testStringMarshallerReadsOnlyTheRange() throws Exception {
      byte[] content = "value".getBytes(UTF_8);

      Object read = new UTF8StringMarshaller().objectFromByteBuffer(pad(content), PADDING, content.length);

      assertEquals("value", read);
   }

   @Test
   public void testJavaSerializationMarshallerReadsOnlyTheRange() throws Exception {
      JavaSerializationMarshaller marshaller = new JavaSerializationMarshaller();
      byte[] content = marshaller.objectToByteBuffer("value");

      Object read = marshaller.objectFromByteBuffer(pad(content), PADDING, content.length);

      assertEquals("value", read);
   }

   /**
    * Places the content in a larger array, with bytes that do not belong to it on both sides.
    *
    * @param content the bytes to place.
    * @return a new array holding the content at {@link #PADDING}.
    */
   private static byte[] pad(byte[] content) {
      byte[] padded = new byte[content.length + 2 * PADDING];
      Arrays.fill(padded, (byte) 0x7F);
      System.arraycopy(content, 0, padded, PADDING, content.length);
      return padded;
   }
}
