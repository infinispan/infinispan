package org.infinispan.client.hotrod.impl.transport.netty;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Signal;

@Test(groups = "unit", testName = "client.hotrod.impl.transport.netty.HintingByteBufTest")
public class HintingByteBufTest {

   public void test2pow7minus1() {
      writeReadInt(127, 1);
   }

   public void test2pow7() {
      writeReadInt(128, 2);
   }

   public void test2pow14minus1() {
      writeReadInt(16383, 2);
   }

   public void test2pow14() {
      writeReadInt(16384, 3);
   }

   public void test2pow21minus1() {
      writeReadInt(2097151, 3);
   }

   public void test2pow21() {
      writeReadInt(2097152, 4);
   }

   public void test2pow28minus1() {
      writeReadInt(268435455, 4);
   }

   public void test2pow28() {
      writeReadInt(268435456, 5);
   }

   public void test2pow35minus1() {
      writeReadLong(34359738367L, 5);
   }

   public void test2pow35() {
      writeReadLong(34359738368L, 6);
   }

   public void test2pow42minus1() {
      writeReadLong(4398046511103L, 6);
   }

   public void test2pow42() {
      writeReadLong(4398046511104L, 7);
   }

   public void test2pow49minus1() {
      writeReadLong(562949953421311L, 7);
   }

   public void test2pow49() {
      writeReadLong(562949953421312L, 8);
   }

   public void test2pow56minus1() {
      writeReadLong(72057594037927935L, 8);
   }

   public void test2pow56() {
      writeReadLong(72057594037927936L, 9);
   }

   public void test2pow63minus1() {
      writeReadLong(9223372036854775807L, 9);
   }

   @Test(expectedExceptions = Signal.class)
   public void testTooLongInt() {
      ByteBuf writer = Unpooled.directBuffer(1024);
      ByteBuf reader = alloc(writer);
      assert(writer.writerIndex() == 0);
      ByteBufUtil.writeVLong(writer, 9223372036854775807L);
      ByteBufUtil.readVInt(reader);
      writer.release();
   }

   private void writeReadInt(int num, int expected) {
      ByteBuf writer = Unpooled.directBuffer(1024);
      ByteBuf reader = alloc(writer);
      assert(writer.writerIndex() == 0);
      ByteBufUtil.writeVInt(writer, num);
      assertEquals(writer.writerIndex(), expected);
      assertEquals(ByteBufUtil.readVInt(reader), num);
      writer.release();
   }

   private void writeReadLong(long num, int expected) {
      ByteBuf writer = Unpooled.directBuffer(1024);
      ByteBuf reader = alloc(writer);
      assert(writer.writerIndex() == 0);
      ByteBufUtil.writeVLong(writer, num);
      assertEquals(expected, writer.writerIndex());
      assertEquals(num, ByteBufUtil.readVLong(reader));
      writer.release();
   }

   private ByteBuf alloc(ByteBuf buf) {
      HintingByteBuf replayable = new HintingByteBuf(new HintedReplayingDecoder<Long>() {
         @Override
         protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            throw new IllegalArgumentException("Should not execute");
         }
      });
      replayable.setCumulation(buf);
      return replayable;
   }
}
