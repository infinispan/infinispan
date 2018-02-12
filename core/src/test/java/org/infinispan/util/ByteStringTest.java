package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "util.ByteStringTest")
public class ByteStringTest extends AbstractInfinispanTest {

   public void testEmptyString() throws Exception {
      ByteString byteString = ByteString.fromString("");
      assertSame(ByteString.emptyString(), byteString);

      ExposedByteArrayOutputStream outputStream = new ExposedByteArrayOutputStream();
      try (ObjectOutput output = new ObjectOutputStream(outputStream)) {
         ByteString.writeObject(output, byteString);
      }
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.getRawBuffer());
      try (ObjectInput input = new ObjectInputStream(inputStream)) {
         ByteString byteString2 = ByteString.readObject(input);
         assertSame(ByteString.emptyString(), byteString2);
      }
   }

   public void testShortString() throws Exception {
      ByteString byteString = ByteString.fromString("abc");

      ExposedByteArrayOutputStream outputStream = new ExposedByteArrayOutputStream();
      try (ObjectOutput output = new ObjectOutputStream(outputStream)) {
         ByteString.writeObject(output, byteString);
      }
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.getRawBuffer());
      try (ObjectInput input = new ObjectInputStream(inputStream)) {
         ByteString byteString2 = ByteString.readObject(input);
         assertEquals(byteString, byteString2);
      }
   }

   public void testLargeString() throws Exception {
      StringBuilder sb = new StringBuilder(128);
      for (int i = 0; i < 128; i++) {
         sb.append("a");
      }
      ByteString.fromString(sb.toString());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testTooLargeString() throws Exception {
      StringBuilder sb = new StringBuilder(256);
      for (int i = 0; i < 256; i++) {
         sb.append("a");
      }
      ByteString.fromString(sb.toString());
   }
}
