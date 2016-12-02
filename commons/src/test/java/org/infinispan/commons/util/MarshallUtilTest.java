package org.infinispan.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.infinispan.commons.marshall.MarshallUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link org.infinispan.commons.marshall.MarshallUtil}
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class MarshallUtilTest {

   private static final int NR_RANDOM = 1000;

   private static void positiveRange(int min, int max, int bytesExpected, ObjectInputOutput io) throws IOException {
      for (int i = min; i > 0 && i < max; i <<= 1) {
         checkIntAndByteArray(i, bytesExpected, io);
      }
   }

   private static void checkIntAndByteArray(int i, int bytesExpected, ObjectInputOutput io) throws IOException {
      io.reset();
      MarshallUtil.marshallSize(io, i);
      Assert.assertEquals("Error for i=" + i, bytesExpected, io.buffer.size());
      Assert.assertEquals("Error for i=" + i, i, MarshallUtil.unmarshallSize(io));
      Assert.assertEquals("Error for i=" + i, 0, io.buffer.size());
   }

   private static void checkNegativeInt(int i, ObjectInputOutput io) throws IOException {
      io.reset();
      MarshallUtil.marshallSize(io, i);
      Assert.assertEquals("Error for i=" + i, 1, io.buffer.size());
      Assert.assertEquals("Error for i=" + i, -1, MarshallUtil.unmarshallSize(io));
      Assert.assertEquals("Error for i=" + i, 0, io.buffer.size());
   }

   @Test
   public void testPositiveRange() throws IOException {
      ObjectInputOutput io = new ObjectInputOutput();
      checkIntAndByteArray(0, 1, io); //zero
      positiveRange(1, 1 << 6, 1, io);
      positiveRange(1 << 6, 1 << 13, 2, io);
      positiveRange(1 << 13, 1 << 20, 3, io);
      positiveRange(1 << 20, 1 << 27, 4, io);
      positiveRange(1 << 27, Integer.MAX_VALUE, 5, io);
      checkIntAndByteArray(Integer.MAX_VALUE, 5, io);
   }

   @Test
   public void testNegativeRange() throws IOException {
      ObjectInputOutput io = new ObjectInputOutput();
      for (int i = 1; i < Integer.MAX_VALUE; i <<= 1) {
         int v = -i;
         if (v >= 0) {
            break;
         }
         checkNegativeInt(v, io);
      }
      checkNegativeInt(Integer.MIN_VALUE, io);
   }

   @Test
   public void testRandomPositiveInt() throws IOException {
      Random random = new Random(System.nanoTime());
      ObjectInputOutput io = new ObjectInputOutput();
      for (int i = 0; i < NR_RANDOM; ++i) {
         int v = random.nextInt();
         if (v < 0) {
            v = -v;
         }
         io.reset();
         MarshallUtil.marshallSize(io, v);
         Assert.assertEquals("Error for v=" + v, v, MarshallUtil.unmarshallSize(io));
      }
   }

   @Test
   public void testRandomNegativeInt() throws IOException {
      Random random = new Random(System.nanoTime());
      ObjectInputOutput io = new ObjectInputOutput();
      for (int i = 0; i < NR_RANDOM; ++i) {
         int v = random.nextInt();
         if (v > 0) {
            v = -v;
         } else if (v == 0) {
            i--;
            continue;
         }
         io.reset();
         MarshallUtil.marshallSize(io, v);
         Assert.assertEquals("Error for v=" + v, -1, MarshallUtil.unmarshallSize(io));
      }
   }

   @Test
   public void testEnum() throws IOException {
      ObjectInputOutput io = new ObjectInputOutput();
      MarshallUtil.marshallEnum(null, io);
      Assert.assertNull(MarshallUtil.unmarshallEnum(io, ordinal -> TestEnum.values()[ordinal]));
      Assert.assertEquals(0, io.buffer.size());

      for (TestEnum e : TestEnum.values()) {
         io.reset();
         MarshallUtil.marshallEnum(e, io);
         Assert.assertEquals(e, MarshallUtil.unmarshallEnum(io, ordinal -> TestEnum.values()[ordinal]));
         Assert.assertEquals(0, io.buffer.size());
      }
   }

   @Test
   public void testUUID() throws IOException {
      ObjectInputOutput io = new ObjectInputOutput();
      MarshallUtil.marshallUUID(null, io, true);
      Assert.assertNull(MarshallUtil.unmarshallUUID(io, true));
      Assert.assertEquals(0, io.buffer.size());

      for (int i = 0; i < NR_RANDOM; ++i) {
         io.reset();
         UUID uuid = UUID.randomUUID();
         MarshallUtil.marshallUUID(uuid, io, false);
         Assert.assertEquals(uuid, MarshallUtil.unmarshallUUID(io, false));
         Assert.assertEquals(0, io.buffer.size());
      }

      for (int i = 0; i < NR_RANDOM; ++i) {
         io.reset();
         UUID uuid = UUID.randomUUID();
         MarshallUtil.marshallUUID(uuid, io, true);
         Assert.assertEquals(uuid, MarshallUtil.unmarshallUUID(io, true));
         Assert.assertEquals(0, io.buffer.size());
      }
   }

   @Test
   public void testArray() throws IOException, ClassNotFoundException {
      ObjectInputOutput io = new ObjectInputOutput();
      MarshallUtil.marshallArray(null, io);
      Assert.assertNull(MarshallUtil.unmarshallArray(io, null));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();

      String[] array = new String[0];
      MarshallUtil.marshallArray(array, io);
      Assert.assertTrue(Arrays.equals(array, MarshallUtil.unmarshallArray(io, String[]::new)));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();

      array = new String[] {"a", "b", "c"};
      MarshallUtil.marshallArray(array, io);
      Assert.assertTrue(Arrays.equals(array, MarshallUtil.unmarshallArray(io, String[]::new)));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();
   }

   @Test
   public void testByteArray() throws IOException, ClassNotFoundException {
      ObjectInputOutput io = new ObjectInputOutput();
      MarshallUtil.marshallByteArray(null, io);
      Assert.assertNull(MarshallUtil.unmarshallByteArray(io));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();

      byte[] array = new byte[0];
      MarshallUtil.marshallByteArray(array, io);
      Assert.assertTrue(Arrays.equals(array, MarshallUtil.unmarshallByteArray(io)));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();

      array = new byte[] {1, 2, 3};
      MarshallUtil.marshallByteArray(array, io);
      Assert.assertTrue(Arrays.equals(array, MarshallUtil.unmarshallByteArray(io)));
      Assert.assertEquals(0, io.buffer.size());
      io.reset();

   }

   private enum TestEnum {
      ONE, TWO, THREE
   }

   private static class ObjectInputOutput implements ObjectOutput, ObjectInput {

      private final Queue<Object> buffer = new LinkedList<>();

      @Override
      public void writeByte(int v) throws IOException {
         buffer.add((byte) v);
      }

      @Override
      public void writeShort(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeChar(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeInt(int v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeLong(long v) throws IOException {
         buffer.add(v);
      }

      @Override
      public void writeFloat(float v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeDouble(double v) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBytes(String s) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeChars(String s) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeUTF(String s) throws IOException {
         throw new UnsupportedOperationException();
      }

      public void reset() {
         buffer.clear();
      }

      @Override
      public Object readObject() throws ClassNotFoundException, IOException {
         return buffer.poll();
      }

      @Override
      public int read() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public long skip(long n) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int available() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void readFully(byte[] b) throws IOException {
         byte[] array = (byte[]) buffer.poll();
         Assert.assertEquals(array.length, b.length);
         System.arraycopy(array, 0, b, 0, b.length);
      }

      @Override
      public void readFully(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int skipBytes(int n) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean readBoolean() throws IOException {
         return (boolean) buffer.poll();
      }

      @Override
      public byte readByte() throws IOException {
         return (byte) buffer.poll();
      }

      @Override
      public int readUnsignedByte() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public short readShort() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readUnsignedShort() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public char readChar() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public int readInt() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public long readLong() throws IOException {
         return (long) buffer.poll();
      }

      @Override
      public float readFloat() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public double readDouble() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readLine() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readUTF() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeObject(Object obj) throws IOException {
         buffer.add(obj);
      }

      @Override
      public void write(int b) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void write(byte[] b) throws IOException {
         buffer.add(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void writeBoolean(boolean v) throws IOException {
         buffer.add(v);
      }

      @Override
      public void flush() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
         throw new UnsupportedOperationException();
      }
   }

}
