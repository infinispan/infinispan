package org.infinispan.server.core

import org.testng.annotations.Test
import org.testng.Assert._
import transport.ExtendedByteBuf._
import org.infinispan.commons.util.Util
import io.netty.buffer.Unpooled

/**
 * Variable length number test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.VariableLengthTest")
class VariableLengthTest {

   def test2pow7minus1 {
      writeReadInt(127, 1)
   }

   def test2pow7 {
      writeReadInt(128, 2)
   }

   def test2pow14minus1 {
      writeReadInt(16383, 2)
   }

   def test2pow14 {
      writeReadInt(16384, 3)
   }

   def test2pow21minus1 {
      writeReadInt(2097151, 3)
   }

   def test2pow21 {
      writeReadInt(2097152, 4)
   }

   def test2pow28minus1 {
      writeReadInt(268435455, 4)
   }

   def test2pow28 {
      writeReadInt(268435456, 5)
   }

   def test2pow35minus1 {
      writeReadLong(34359738367L, 5)
   }

   def test2pow35 {
      writeReadLong(34359738368L, 6)
   }

   def test2pow42minus1 {
      writeReadLong(4398046511103L, 6)
   }

   def test2pow42 {
      writeReadLong(4398046511104L, 7)
   }

   def test2pow49minus1 {
      writeReadLong(562949953421311L, 7)
   }

   def test2pow49 {
      writeReadLong(562949953421312L, 8)
   }

   def test2pow56minus1 {
      writeReadLong(72057594037927935L, 8)
   }

   def test2pow56 {
      writeReadLong(72057594037927936L, 9)
   }

   def test2pow63minus1 {
      writeReadLong(9223372036854775807L, 9)
   }

   @Test(expectedExceptions = Array(classOf[IllegalStateException]))
   def testTooLongInt {
      val buffer = Unpooled.directBuffer(1024)
      assert(buffer.writerIndex == 0)
      writeUnsignedLong(9223372036854775807L, buffer)
      readUnsignedInt(buffer)
      buffer.release()
   }

   @Test(groups = Array("unstable"))
   def testPrintHexadecimalVint {
      val buffer = Unpooled.directBuffer(1024)
      assert(buffer.writerIndex == 0)
      writeUnsignedLong(512, buffer)
      println(Util.hexDump(buffer.nioBuffer()))
      println
      buffer.release()
   }

//   def test2pow63() {
//      writeReadLong(9223372036854775808L, 10)
//   }

   private def writeReadInt(num: Int, expected: Int) {
      val buffer = Unpooled.directBuffer(1024)
      assert(buffer.writerIndex == 0)
      writeUnsignedInt(num, buffer)
      assertEquals(buffer.writerIndex, expected)
      assertEquals(readUnsignedInt(buffer), num)
      buffer.release()
   }

   private def writeReadLong(num: Long, expected: Int) {
      val buffer = Unpooled.directBuffer(1024)
      assert(buffer.writerIndex == 0)
      writeUnsignedLong(num, buffer)
      assertEquals(buffer.writerIndex, expected)
      assertEquals(readUnsignedLong(buffer), num)
      buffer.release()
   }

}
