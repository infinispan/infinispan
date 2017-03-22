package org.infinispan.marshall.core;

import java.io.IOException;

import org.jboss.marshalling.util.IdentityIntMap;

final class Primitives {

   // 0x03-0x0F reserved
   static final int ID_BYTE_ARRAY                  = 0x01; // byte[].class
   static final int ID_STRING                      = 0x02; // String.class

   static final int ID_BOOLEAN_OBJ                 = 0x10; // Boolean.class
   static final int ID_BYTE_OBJ                    = 0x11; // ..etc..
   static final int ID_CHAR_OBJ                    = 0x12;
   static final int ID_DOUBLE_OBJ                  = 0x13;
   static final int ID_FLOAT_OBJ                   = 0x14;
   static final int ID_INT_OBJ                     = 0x15;
   static final int ID_LONG_OBJ                    = 0x16;
   static final int ID_SHORT_OBJ                   = 0x17;

   static final int ID_BOOLEAN_ARRAY               = 0x18; // boolean[].class
   static final int ID_CHAR_ARRAY                  = 0x19; // ..etc..
   static final int ID_DOUBLE_ARRAY                = 0x1A;
   static final int ID_FLOAT_ARRAY                 = 0x1B;
   static final int ID_INT_ARRAY                   = 0x1C;
   static final int ID_LONG_ARRAY                  = 0x1D;
   static final int ID_SHORT_ARRAY                 = 0x1E;

   // 0x1F-0x27 unused

   static final int ID_ARRAY_EMPTY                 = 0x28; // zero elements
   static final int ID_ARRAY_SMALL                 = 0x29; // <=0x100 elements
   static final int ID_ARRAY_MEDIUM                = 0x2A; // <=0x10000 elements
   static final int ID_ARRAY_LARGE                 = 0x2B; // <0x80000000 elements

   static final int SMALL_ARRAY_MIN                = 0x1;
   static final int SMALL_ARRAY_MAX                = 0x100;
   static final int MEDIUM_ARRAY_MIN               = 0x101;
   static final int MEDIUM_ARRAY_MAX               = 0x10100;

   static final IdentityIntMap<Class<?>> PRIMITIVES = new IdentityIntMap<>(0x0.6p0f);

   static {
      PRIMITIVES.put(String.class, ID_STRING);
      PRIMITIVES.put(byte[].class, ID_BYTE_ARRAY);

      PRIMITIVES.put(Boolean.class, ID_BOOLEAN_OBJ);
      PRIMITIVES.put(Byte.class, ID_BYTE_OBJ);
      PRIMITIVES.put(Character.class, ID_CHAR_OBJ);
      PRIMITIVES.put(Double.class, ID_DOUBLE_OBJ);
      PRIMITIVES.put(Float.class, ID_FLOAT_OBJ);
      PRIMITIVES.put(Integer.class, ID_INT_OBJ);
      PRIMITIVES.put(Long.class, ID_LONG_OBJ);
      PRIMITIVES.put(Short.class, ID_SHORT_OBJ);

      PRIMITIVES.put(boolean[].class, ID_BOOLEAN_ARRAY);
      PRIMITIVES.put(char[].class, ID_CHAR_ARRAY);
      PRIMITIVES.put(double[].class, ID_DOUBLE_ARRAY);
      PRIMITIVES.put(float[].class, ID_FLOAT_ARRAY);
      PRIMITIVES.put(int[].class, ID_INT_ARRAY);
      PRIMITIVES.put(long[].class, ID_LONG_ARRAY);
      PRIMITIVES.put(short[].class, ID_SHORT_ARRAY);
   }

   private Primitives() {
   }

   static void writePrimitive(Object obj, BytesObjectOutput out, int id) throws IOException {
      out.writeByte(id);
      writeRawPrimitive(obj, out, id);
   }

   static void writeRawPrimitive(Object obj, BytesObjectOutput out, int id) throws IOException {
      switch (id) {
         case ID_BYTE_ARRAY:
            Primitives.writeByteArray((byte[]) obj, out);
            break;
         case ID_STRING:
            out.writeString((String) obj);
            break;
         case ID_BOOLEAN_OBJ:
            out.writeBoolean((boolean) obj);
            break;
         case ID_BYTE_OBJ:
            out.writeByte((byte) obj);
            break;
         case ID_CHAR_OBJ:
            out.writeChar((char) obj);
            break;
         case ID_DOUBLE_OBJ:
            out.writeDouble((double) obj);
            break;
         case ID_FLOAT_OBJ:
            out.writeFloat((float) obj);
            break;
         case ID_INT_OBJ:
            out.writeInt((int) obj);
            break;
         case ID_LONG_OBJ:
            out.writeLong((long) obj);
            break;
         case ID_SHORT_OBJ:
            out.writeShort((short) obj);
            break;
         case ID_BOOLEAN_ARRAY:
            Primitives.writeBooleanArray((boolean[]) obj, out);
            break;
         case ID_CHAR_ARRAY:
            Primitives.writeCharArray((char[]) obj, out);
            break;
         case ID_DOUBLE_ARRAY:
            Primitives.writeDoubleArray((double[]) obj, out);
            break;
         case ID_FLOAT_ARRAY:
            Primitives.writeFloatArray((float[]) obj, out);
            break;
         case ID_INT_ARRAY:
            Primitives.writeIntArray((int[]) obj, out);
            break;
         case ID_LONG_ARRAY:
            Primitives.writeLongArray((long[]) obj, out);
            break;
         case ID_SHORT_ARRAY:
            Primitives.writeShortArray((short[]) obj, out);
            break;
         default:
            throw new IOException("Unknown primitive type: " + obj);
      }
   }

   static Object readPrimitive(BytesObjectInput in) throws IOException, ClassNotFoundException {
      int subId = in.readUnsignedByte();
      return readRawPrimitive(in, subId);
   }

   static Object readRawPrimitive(BytesObjectInput in, int subId) throws IOException {
      switch (subId) {
         case ID_BYTE_ARRAY:
            return readByteArray(in);
         case ID_STRING:
            return in.readString();
         case ID_BOOLEAN_OBJ:
            return in.readBoolean();
         case ID_BYTE_OBJ:
            return in.readByte();
         case ID_CHAR_OBJ:
            return in.readChar();
         case ID_DOUBLE_OBJ:
            return in.readDouble();
         case ID_FLOAT_OBJ:
            return in.readFloat();
         case ID_INT_OBJ:
            return in.readInt();
         case ID_LONG_OBJ:
            return in.readLong();
         case ID_SHORT_OBJ:
            return in.readShort();
         case ID_BOOLEAN_ARRAY:
            return readBooleanArray(in);
         case ID_CHAR_ARRAY:
            return readCharArray(in);
         case ID_DOUBLE_ARRAY:
            return readDoubleArray(in);
         case ID_FLOAT_ARRAY:
            return readFloatArray(in);
         case ID_INT_ARRAY:
            return readIntArray(in);
         case ID_LONG_ARRAY:
            return readLongArray(in);
         case ID_SHORT_ARRAY:
            return readShortArray(in);
         default:
            throw new IOException("Unknown primitive sub id: " + Integer.toHexString(subId));
      }
   }

   private static void writeByteArray(byte[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         out.write(obj, 0, len);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         out.write(obj, 0, len);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         out.write(obj, 0, len);
      }
   }

   private static void writeBooleanArray(boolean[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         writeBooleans(obj, out);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         writeBooleans(obj, out);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         writeBooleans(obj, out);
      }
   }

   private static void writeBooleans(boolean[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      final int bc = len & ~7;
      for (int i = 0; i < bc;) {
         out.write(
               (obj[i++] ? 1 : 0)
                     | (obj[i++] ? 2 : 0)
                     | (obj[i++] ? 4 : 0)
                     | (obj[i++] ? 8 : 0)
                     | (obj[i++] ? 16 : 0)
                     | (obj[i++] ? 32 : 0)
                     | (obj[i++] ? 64 : 0)
                     | (obj[i++] ? 128 : 0)
         );
      }
      if (bc < len) {
         int o = 0;
         int bit = 1;
         for (int i = bc; i < len; i++) {
            if (obj[i]) o |= bit;
            bit <<= 1;
         }
         out.writeByte(o);
      }
   }

   private static void writeCharArray(char[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (char v : obj) out.writeChar(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (char v : obj) out.writeChar(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (char v : obj) out.writeChar(v);
      }
   }

   private static void writeDoubleArray(double[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (double v : obj) out.writeDouble(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (double v : obj) out.writeDouble(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (double v : obj) out.writeDouble(v);
      }
   }

   private static void writeFloatArray(float[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (float v : obj) out.writeFloat(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (float v : obj) out.writeFloat(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (float v : obj) out.writeFloat(v);
      }
   }

   private static void writeIntArray(int[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (int v : obj) out.writeInt(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (int v : obj) out.writeInt(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (int v : obj) out.writeInt(v);
      }
   }

   private static void writeLongArray(long[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (long v : obj) out.writeLong(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (long v : obj) out.writeLong(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (long v : obj) out.writeLong(v);
      }
   }

   private static void writeShortArray(short[] obj, BytesObjectOutput out) {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ID_ARRAY_EMPTY);
      } else if (len <= SMALL_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_SMALL);
         out.writeByte(len - SMALL_ARRAY_MIN);
         for (short v : obj) out.writeShort(v);
      } else if (len <= MEDIUM_ARRAY_MAX) {
         out.writeByte(ID_ARRAY_MEDIUM);
         out.writeShort(len - MEDIUM_ARRAY_MIN);
         for (short v : obj) out.writeShort(v);
      } else {
         out.writeByte(ID_ARRAY_LARGE);
         out.writeInt(len);
         for (short v : obj) out.writeShort(v);
      }
   }

   private static byte[] readByteArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new byte[]{};
         case ID_ARRAY_SMALL:
            return readFully(mkByteArray(in.readUnsignedByte() + SMALL_ARRAY_MIN), in);
         case ID_ARRAY_MEDIUM:
            return readFully(mkByteArray(in.readUnsignedShort() + MEDIUM_ARRAY_MIN), in);
         case ID_ARRAY_LARGE:
            return readFully(new byte[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static byte[] mkByteArray(int len) {
      return new byte[len];
   }

   private static byte[] readFully(byte[] arr, BytesObjectInput in) {
      in.readFully(arr);
      return arr;
   }

   private static boolean[] readBooleanArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new boolean[]{};
         case ID_ARRAY_SMALL:
            return readBooleans(mkBooleanArray(in.readUnsignedByte() + SMALL_ARRAY_MIN), in);
         case ID_ARRAY_MEDIUM:
            return readBooleans(mkBooleanArray(in.readUnsignedShort() + MEDIUM_ARRAY_MIN), in);
         case ID_ARRAY_LARGE:
            return readBooleans(new boolean[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static boolean[] mkBooleanArray(int len) {
      return new boolean[len];
   }

   private static boolean[] readBooleans(boolean[] arr, BytesObjectInput in) {
      final int len = arr.length;
      int v;
      int bc = len & ~7;
      for (int i = 0; i < bc; ) {
         v = in.readByte();
         arr[i++] = (v & 1) != 0;
         arr[i++] = (v & 2) != 0;
         arr[i++] = (v & 4) != 0;
         arr[i++] = (v & 8) != 0;
         arr[i++] = (v & 16) != 0;
         arr[i++] = (v & 32) != 0;
         arr[i++] = (v & 64) != 0;
         arr[i++] = (v & 128) != 0;
      }
      if (bc < len) {
         v = in.readByte();
         switch (len & 7) {
            case 7:
               arr[bc + 6] = (v & 64) != 0;
            case 6:
               arr[bc + 5] = (v & 32) != 0;
            case 5:
               arr[bc + 4] = (v & 16) != 0;
            case 4:
               arr[bc + 3] = (v & 8) != 0;
            case 3:
               arr[bc + 2] = (v & 4) != 0;
            case 2:
               arr[bc + 1] = (v & 2) != 0;
            case 1:
               arr[bc] = (v & 1) != 0;
         }
      }
      return arr;
   }

   private static char[] readCharArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new char[]{};
         case ID_ARRAY_SMALL:
            return readChars(mkCharArray(in.readUnsignedByte() + SMALL_ARRAY_MIN), in);
         case ID_ARRAY_MEDIUM:
            return readChars(mkCharArray(in.readUnsignedShort() + MEDIUM_ARRAY_MIN), in);
         case ID_ARRAY_LARGE:
            return readChars(new char[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static char[] mkCharArray(int len) {
      return new char[len];
   }

   private static char[] readChars(char[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readChar();
      return arr;
   }

   private static double[] readDoubleArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new double[]{};
         case ID_ARRAY_SMALL:
            return readDoubles(new double[in.readUnsignedByte() + SMALL_ARRAY_MIN], in);
         case ID_ARRAY_MEDIUM:
            return readDoubles(new double[in.readUnsignedShort() + MEDIUM_ARRAY_MIN], in);
         case ID_ARRAY_LARGE:
            return readDoubles(new double[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static double[] readDoubles(double[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readDouble();
      return arr;
   }

   private static float[] readFloatArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new float[]{};
         case ID_ARRAY_SMALL:
            return readFloats(new float[in.readUnsignedByte() + SMALL_ARRAY_MIN], in);
         case ID_ARRAY_MEDIUM:
            return readFloats(new float[in.readUnsignedShort() + MEDIUM_ARRAY_MIN], in);
         case ID_ARRAY_LARGE:
            return readFloats(new float[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static float[] readFloats(float[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readFloat();
      return arr;
   }

   private static int[] readIntArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new int[]{};
         case ID_ARRAY_SMALL:
            return readInts(new int[in.readUnsignedByte() + SMALL_ARRAY_MIN], in);
         case ID_ARRAY_MEDIUM:
            return readInts(new int[in.readUnsignedShort() + MEDIUM_ARRAY_MIN], in);
         case ID_ARRAY_LARGE:
            return readInts(new int[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static int[] readInts(int[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readInt();
      return arr;
   }

   private static long[] readLongArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new long[]{};
         case ID_ARRAY_SMALL:
            return readLongs(new long[in.readUnsignedByte() + SMALL_ARRAY_MIN], in);
         case ID_ARRAY_MEDIUM:
            return readLongs(new long[in.readUnsignedShort() + MEDIUM_ARRAY_MIN], in);
         case ID_ARRAY_LARGE:
            return readLongs(new long[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static long[] readLongs(long[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readLong();
      return arr;
   }

   private static short[] readShortArray(BytesObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ID_ARRAY_EMPTY:
            return new short[]{};
         case ID_ARRAY_SMALL:
            return readShorts(new short[in.readUnsignedByte() + SMALL_ARRAY_MIN], in);
         case ID_ARRAY_MEDIUM:
            return readShorts(new short[in.readUnsignedShort() + MEDIUM_ARRAY_MIN], in);
         case ID_ARRAY_LARGE:
            return readShorts(new short[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private static short[] readShorts(short[] arr, BytesObjectInput in) {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readShort();
      return arr;
   }
}
