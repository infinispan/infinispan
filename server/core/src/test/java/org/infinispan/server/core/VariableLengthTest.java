package org.infinispan.server.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.infinispan.commons.util.Util;
import org.testng.annotations.Test;

import static org.infinispan.server.core.transport.ExtendedByteBuf.readUnsignedInt;
import static org.infinispan.server.core.transport.ExtendedByteBuf.readUnsignedLong;
import static org.infinispan.server.core.transport.ExtendedByteBuf.writeUnsignedInt;
import static org.infinispan.server.core.transport.ExtendedByteBuf.writeUnsignedLong;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Variable length number test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.VariableLengthTest")
public class VariableLengthTest {

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

   @Test(expectedExceptions = IllegalStateException.class)
   public void testTooLongInt() {
      ByteBuf buffer = Unpooled.directBuffer(1024);
      assert(buffer.writerIndex() == 0);
      writeUnsignedLong(9223372036854775807L, buffer);
      readUnsignedInt(buffer);
      buffer.release();
   }

   @Test(groups = "unstable")
   public void testPrintHexadecimalVint() {
      ByteBuf buffer = Unpooled.directBuffer(1024);
      assert(buffer.writerIndex() == 0);
      writeUnsignedLong(512, buffer);
      System.out.println(Util.hexDump(buffer.nioBuffer()));
      System.out.println();
      buffer.release();
   }

//   public void test2pow63() {
//      writeReadLong(9223372036854775808L, 10)
//   }

   private void writeReadInt(int num, int expected) {
      ByteBuf buffer = Unpooled.directBuffer(1024);
      assert(buffer.writerIndex() == 0);
      writeUnsignedInt(num, buffer);
      assertEquals(buffer.writerIndex(), expected);
      assertEquals(readUnsignedInt(buffer), num);
      buffer.release();
   }

   private void writeReadLong(long num, int expected) {
      ByteBuf buffer = Unpooled.directBuffer(1024);
      assert(buffer.writerIndex() == 0);
      writeUnsignedLong(num, buffer);
      assertEquals(buffer.writerIndex(), expected);
      assertEquals(readUnsignedLong(buffer), num);
      buffer.release();
   }

}
