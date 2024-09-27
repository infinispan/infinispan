package org.infinispan.server.memcached.text;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Test(groups = "unit", testName = "server.memcached.text.TextIntrinsicsTest")
public class TextIntrinsicsTest {

   private final TokenReader reader = new TokenReader(Unpooled.buffer(256));

   @AfterClass
   protected void clearAfter() {
      reader.release();
   }

   public void testKeyList() {
      byte[] datum = "key1 key2\r\n".getBytes(StandardCharsets.US_ASCII);

      ByteBuf buf = Unpooled.wrappedBuffer(datum);
      List<byte[]> keys = TextIntrinsics.text_key_list(buf, reader);

      assertThat(buf.isReadable()).isFalse();
      assertThat(keys).hasSize(2);
   }

   public void testIncompleteKeyList() {
      byte[] datum = "key1 ke".getBytes(StandardCharsets.US_ASCII);

      ByteBuf buf = Unpooled.wrappedBuffer(datum);
      List<byte[]> keys = TextIntrinsics.text_key_list(buf, reader);

      assertThat(buf.readerIndex()).isZero();
      assertThat(keys).hasSize(0);
   }
}
