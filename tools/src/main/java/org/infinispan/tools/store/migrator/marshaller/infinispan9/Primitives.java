package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.io.EOFException;
import java.io.IOException;

final class Primitives {

   // 0x03-0x0F reserved
   private static final int ID_BYTE_ARRAY                  = 0x01; // byte[].class
   private static final int ID_STRING                      = 0x02; // String.class

   private static final int ID_BOOLEAN_OBJ                 = 0x10; // Boolean.class
   private static final int ID_BYTE_OBJ                    = 0x11; // ..etc..
   private static final int ID_CHAR_OBJ                    = 0x12;
   private static final int ID_DOUBLE_OBJ                  = 0x13;
   private static final int ID_FLOAT_OBJ                   = 0x14;
   private static final int ID_INT_OBJ                     = 0x15;
   private static final int ID_LONG_OBJ                    = 0x16;
   private static final int ID_SHORT_OBJ                   = 0x17;

   private static final int ID_BOOLEAN_ARRAY               = 0x18; // boolean[].class
   private static final int ID_CHAR_ARRAY                  = 0x19; // ..etc..
   private static final int ID_DOUBLE_ARRAY                = 0x1A;
   private static final int ID_FLOAT_ARRAY                 = 0x1B;
   private static final int ID_INT_ARRAY                   = 0x1C;
   private static final int ID_LONG_ARRAY                  = 0x1D;
   private static final int ID_SHORT_ARRAY                 = 0x1E;

   // 0x1F-0x27 unused

   private static final int ID_ARRAY_EMPTY                 = 0x28; // zero elements
   private static final int ID_ARRAY_SMALL                 = 0x29; // <=0x100 elements
   private static final int ID_ARRAY_MEDIUM                = 0x2A; // <=0x10000 elements
   private static final int ID_ARRAY_LARGE                 = 0x2B; // <0x80000000 elements

   static final int SMALL_ARRAY_MIN                = 0x1;
   static final int MEDIUM_ARRAY_MIN               = 0x101;

   private Primitives() {
   }

   static Object readPrimitive(BytesObjectInput in) throws IOException {
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

   private static byte[] readFully(byte[] arr, BytesObjectInput in) throws EOFException {
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

   private static boolean[] readBooleans(boolean[] arr, BytesObjectInput in) throws EOFException {
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

   private static char[] readChars(char[] arr, BytesObjectInput in) throws EOFException {
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

   private static double[] readDoubles(double[] arr, BytesObjectInput in) throws EOFException {
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

   private static float[] readFloats(float[] arr, BytesObjectInput in) throws EOFException {
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

   private static int[] readInts(int[] arr, BytesObjectInput in) throws EOFException {
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

   private static long[] readLongs(long[] arr, BytesObjectInput in) throws EOFException {
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

   private static short[] readShorts(short[] arr, BytesObjectInput in) throws EOFException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readShort();
      return arr;
   }
}
