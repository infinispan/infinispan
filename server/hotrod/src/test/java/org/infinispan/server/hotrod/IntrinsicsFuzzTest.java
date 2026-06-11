package org.infinispan.server.hotrod;

import org.infinispan.testing.jupiter.tags.Fuzz;

import com.code_intelligence.jazzer.junit.FuzzTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Fuzz
public class IntrinsicsFuzzTest {

   @FuzzTest(maxDuration = "5m")
   void fuzzVInt(byte[] data) {
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      try {
         Intrinsics.vInt(buf);
      } finally {
         buf.release();
      }
   }

   @FuzzTest(maxDuration = "5m")
   void fuzzVLong(byte[] data) {
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      try {
         Intrinsics.vLong(buf);
      } finally {
         buf.release();
      }
   }

   @FuzzTest(maxDuration = "5m")
   void fuzzString(byte[] data) {
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      try {
         Intrinsics.string(buf, 1024);
      } finally {
         buf.release();
      }
   }

   @FuzzTest(maxDuration = "5m")
   void fuzzArray(byte[] data) {
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      try {
         Intrinsics.array(buf, 1024);
      } catch (io.netty.handler.codec.TooLongFrameException e) {
         // expected for oversized arrays
      } catch (NegativeArraySizeException e) {
         // expected for random vints
      } finally {
         buf.release();
      }
   }
}
