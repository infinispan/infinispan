package org.infinispan.commons.io;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class SignedNumericTest {

   @Test
   public void testEncodeDecode() throws Exception {
      encodeDecode(-Integer.MAX_VALUE, 5);
      encodeDecode(-2500000, 4);
      encodeDecode(-15000, 3);
      encodeDecode(-100, 2);
      encodeDecode(-1, 1);
      encodeDecode(0, 1);
      encodeDecode(1, 1);
      encodeDecode(60, 1);
      encodeDecode(128, 2);
      encodeDecode(1300, 2);
      encodeDecode(15000, 3);
      encodeDecode(2500000, 4);
      encodeDecode(Integer.MAX_VALUE - 1, 5);
   }

   private void encodeDecode(int value, int expectedSize) throws IOException {
      try (ByteArrayOutputStream os = new ByteArrayOutputStream(5)) {
         SignedNumeric.writeSignedInt(os, value);
         byte[] bytes = os.toByteArray();
         try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            int read = SignedNumeric.readSignedInt(is);
            assertEquals(read, value);
         }
         assertEquals(bytes.length, expectedSize);
      }
   }

}
