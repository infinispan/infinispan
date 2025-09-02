package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.infinispan.marshall.persistence.impl.PersistenceContextInitializer;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "unit", testName = "util.ByteStringTest")
public class ByteStringTest extends AbstractInfinispanTest {

   public void testEmptyString() throws Exception {
      SerializationContext ctx = ctx();
      ByteString byteString = ByteString.fromString("");
      assertSame(ByteString.emptyString(), byteString);

      byte[] bytes = ProtobufUtil.toWrappedByteArray(ctx, byteString);
      ByteString byteString2 = ProtobufUtil.fromWrappedByteArray(ctx, bytes);
      assertSame(ByteString.emptyString(), byteString2);

      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
         ProtobufUtil.toWrappedStream(ctx, outputStream, byteString);

         try (InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            byteString2 = ProtobufUtil.fromWrappedStream(ctx, inputStream);
            assertSame(ByteString.emptyString(), byteString2);
         }
      }
   }

   public void testShortString() throws Exception {
      SerializationContext ctx = ctx();
      ByteString byteString = ByteString.fromString("abc");

      byte[] bytes = ProtobufUtil.toWrappedByteArray(ctx, byteString);
      ByteString byteString2 = ProtobufUtil.fromWrappedByteArray(ctx, bytes);
      assertEquals(byteString, byteString2);

      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
         ProtobufUtil.toWrappedStream(ctx, outputStream, byteString);

         try (InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
            byteString2 = ProtobufUtil.fromWrappedStream(ctx, inputStream);
            assertEquals(byteString, byteString2);
         }
      }
   }

   public void testLargeString() throws Exception {
      ByteString.fromString("a".repeat(128));
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testTooLargeString() throws Exception {
      ByteString.fromString("a".repeat(256));
   }

   private SerializationContext ctx() {
      SerializationContext ctx = ProtobufUtil.newSerializationContext();
      PersistenceContextInitializer.INSTANCE.register(ctx);
      return ctx;
   }
}
