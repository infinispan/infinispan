package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.jboss.marshalling.util.IdentityIntMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

final class PrimitiveExternalizer implements AdvancedExternalizer<Object> {

   static final int SMALL                       = 0x100;
   static final int MEDIUM                      = 0x10000;

   // 0x03-0x0F reserved
   static final int NULL                        = 0x00;
   static final int BYTE_ARRAY                  = 0x01; // byte[].class
   static final int STRING                      = 0x02; // String.class

   static final int BOOLEAN_OBJ                 = 0x10; // Boolean.class
   static final int BYTE_OBJ                    = 0x11; // ..etc..
   static final int CHAR_OBJ                    = 0x12;
   static final int DOUBLE_OBJ                  = 0x13;
   static final int FLOAT_OBJ                   = 0x14;
   static final int INT_OBJ                     = 0x15;
   static final int LONG_OBJ                    = 0x16;
   static final int SHORT_OBJ                   = 0x17;

   static final int BOOLEAN_ARRAY               = 0x18; // boolean[].class
   static final int CHAR_ARRAY                  = 0x19; // ..etc..
   static final int DOUBLE_ARRAY                = 0x1A;
   static final int FLOAT_ARRAY                 = 0x1B;
   static final int INT_ARRAY                   = 0x1C;
   static final int LONG_ARRAY                  = 0x1D;
   static final int SHORT_ARRAY                 = 0x1E;

   static final int OBJECT_ARRAY                 = 0x1F; // Object[]

   static final int ARRAY_EMPTY                 = 0x28; // zero elements
   static final int ARRAY_SMALL                 = 0x29; // <=0x100 elements
   static final int ARRAY_MEDIUM                = 0x2A; // <=0x10000 elements
   static final int ARRAY_LARGE                 = 0x2B; // <0x80000000 elements

   // 0x2C-0x2F unused

   final IdentityIntMap<Class<?>> subIds = new IdentityIntMap<>(16);
   final Set<Class<?>> types = new PrimitiveTypes();

   // Assumes that whatever encoding passed can handle ObjectOutput/ObjectInput
   // (that might change when adding support for NIO ByteBuffers)
   final Encoding enc;

   public PrimitiveExternalizer(Encoding enc) {
      this.enc = enc;
      subIds.put(String.class, STRING);
      subIds.put(byte[].class, BYTE_ARRAY);

      subIds.put(Boolean.class, BOOLEAN_OBJ);
      subIds.put(Byte.class, BYTE_OBJ);
      subIds.put(Character.class, CHAR_OBJ);
      subIds.put(Double.class, DOUBLE_OBJ);
      subIds.put(Float.class, FLOAT_OBJ);
      subIds.put(Integer.class, INT_OBJ);
      subIds.put(Long.class, LONG_OBJ);
      subIds.put(Short.class, SHORT_OBJ);

      subIds.put(boolean[].class, BOOLEAN_ARRAY);
      subIds.put(char[].class, CHAR_ARRAY);
      subIds.put(double[].class, DOUBLE_ARRAY);
      subIds.put(float[].class, FLOAT_ARRAY);
      subIds.put(int[].class, INT_ARRAY);
      subIds.put(long[].class, LONG_ARRAY);
      subIds.put(short[].class, SHORT_ARRAY);

      subIds.put(Object[].class, OBJECT_ARRAY);
   }

   @Override
   public Set<Class<?>> getTypeClasses() {
      return types;
   }

   @Override
   public void writeObject(ObjectOutput out, Object obj) throws IOException {
      if (obj == null) {
         out.writeByte(NULL);
      } else {
         int subId = subIds.get(obj.getClass(), -1);
         out.writeByte(subId);
         switch (subId) {
            case BYTE_ARRAY:
               writeByteArray((byte[]) obj, out);
               break;
            case STRING:
               writeString(out, (String) obj);
               break;
            case BOOLEAN_OBJ:
               out.writeBoolean((boolean) obj);
               break;
            case BYTE_OBJ:
               out.writeByte((int) obj);
               break;
            case CHAR_OBJ:
               out.writeChar((int) obj);
               break;
            case DOUBLE_OBJ:
               out.writeDouble((double) obj);
               break;
            case FLOAT_OBJ:
               out.writeFloat((float) obj);
               break;
            case INT_OBJ:
               out.writeInt((int) obj);
               break;
            case LONG_OBJ:
               out.writeLong((long) obj);
               break;
            case SHORT_OBJ:
               out.writeShort((int) obj);
               break;
            case BOOLEAN_ARRAY:
               writeBooleanArray((boolean[]) obj, out);
               break;
            case CHAR_ARRAY:
               writeCharArray((char[]) obj, out);
               break;
            case DOUBLE_ARRAY:
               writeDoubleArray((double[]) obj, out);
               break;
            case FLOAT_ARRAY:
               writeFloatArray((float[]) obj, out);
               break;
            case INT_ARRAY:
               writeIntArray((int[]) obj, out);
               break;
            case LONG_ARRAY:
               writeLongArray((long[]) obj, out);
               break;
            case SHORT_ARRAY:
               writeShortArray((short[]) obj, out);
               break;
            case OBJECT_ARRAY:
               writeObjectArray((Object[]) obj, out);
               break;
            default:
               throw new IOException("Unknown primitive type: " + obj);
         }
      }
   }

   @SuppressWarnings("unchecked")
   private void writeString(ObjectOutput out, String obj) {
      // Instead of out.writeUTF() to be able to write smaller String payloads
      enc.encodeString(obj, out);
   }

   private void writeByteArray(byte[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         out.write(obj, 0, len);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         out.write(obj, 0, len);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         out.write(obj, 0, len);
      }
   }

   private void writeBooleanArray(boolean[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         writeBooleans(obj, out);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         writeBooleans(obj, out);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         writeBooleans(obj, out);
      }
   }

   private void writeBooleans(boolean[] obj, ObjectOutput out) throws IOException {
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

   private void writeCharArray(char[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (char v : obj) out.writeChar(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (char v : obj) out.writeChar(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (char v : obj) out.writeChar(v);
      }
   }

   private void writeDoubleArray(double[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (double v : obj) out.writeDouble(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (double v : obj) out.writeDouble(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (double v : obj) out.writeDouble(v);
      }
   }

   private void writeFloatArray(float[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (float v : obj) out.writeFloat(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (float v : obj) out.writeFloat(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (float v : obj) out.writeFloat(v);
      }
   }

   private void writeIntArray(int[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (int v : obj) out.writeInt(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (int v : obj) out.writeInt(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (int v : obj) out.writeInt(v);
      }
   }

   private void writeLongArray(long[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (long v : obj) out.writeLong(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (long v : obj) out.writeLong(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (long v : obj) out.writeLong(v);
      }
   }

   private void writeShortArray(short[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (short v : obj) out.writeShort(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (short v : obj) out.writeShort(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (short v : obj) out.writeShort(v);
      }
   }

   private void writeObjectArray(Object[] obj, ObjectOutput out) throws IOException {
      final int len = obj.length;
      if (len == 0) {
         out.writeByte(ARRAY_EMPTY);
      } else if (len <= 256) {
         out.writeByte(ARRAY_SMALL);
         out.writeByte(len);
         for (Object v : obj) out.writeObject(v);
      } else if (len <= 65536) {
         out.writeByte(ARRAY_MEDIUM);
         out.writeShort(len);
         for (Object v : obj) out.writeObject(v);
      } else {
         out.writeByte(ARRAY_LARGE);
         out.writeInt(len);
         for (Object v : obj) out.writeObject(v);
      }
   }

   @Override
   public Object readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      int subId = in.readUnsignedByte();
      switch (subId) {
         case NULL:
            return null;
         case BYTE_ARRAY:
            return readByteArray(in);
         case STRING:
            return readString(in);
         case BOOLEAN_OBJ:
            return in.readBoolean();
         case BYTE_OBJ:
            return in.readByte();
         case CHAR_OBJ:
            return in.readChar();
         case DOUBLE_OBJ:
            return in.readDouble();
         case FLOAT_OBJ:
            return in.readFloat();
         case INT_OBJ:
            return in.readInt();
         case LONG_OBJ:
            return in.readLong();
         case SHORT_OBJ:
            return in.readShort();
         case BOOLEAN_ARRAY:
            return readBooleanArray(in);
         case CHAR_ARRAY:
            return readCharArray(in);
         case DOUBLE_ARRAY:
            return readDoubleArray(in);
         case FLOAT_ARRAY:
            return readFloatArray(in);
         case INT_ARRAY:
            return readIntArray(in);
         case LONG_ARRAY:
            return readLongArray(in);
         case SHORT_ARRAY:
            return readShortArray(in);
         case OBJECT_ARRAY:
            return readObjectArray(in);
         default:
            throw new IOException("Unknown primitive sub id: " + Integer.toHexString(subId));
      }
   }

   @SuppressWarnings("unchecked")
   private String readString(ObjectInput in) {
      return enc.decodeString(in); // Counterpart to Encoding.encodeString()
   }

   private byte[] readByteArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new byte[]{};
         case ARRAY_SMALL:
            return readFully(mkByteArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readFully(mkByteArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readFully(new byte[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private byte[] mkByteArray(int len, int limit) {
      return new byte[len == 0 ? limit : len];
   }

   private byte[] readFully(byte[] arr, ObjectInput in) throws IOException {
      in.readFully(arr);
      return arr;
   }

   private boolean[] readBooleanArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      int len;
      switch (type) {
         case ARRAY_EMPTY:
            return new boolean[]{};
         case ARRAY_SMALL:
            return readBooleans(mkBooleanArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readBooleans(mkBooleanArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readBooleans(new boolean[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private boolean[] mkBooleanArray(int len, int limit) {
      return new boolean[len == 0 ? limit : len];
   }

   private boolean[] readBooleans(boolean[] arr, ObjectInput in) throws IOException {
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

   private char[] readCharArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new char[]{};
         case ARRAY_SMALL:
            return readChars(mkCharArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readChars(mkCharArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readChars(new char[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private char[] mkCharArray(int len, int limit) {
      return new char[len == 0 ? limit : len];
   }

   private char[] readChars(char[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readChar();
      return arr;
   }

   private double[] readDoubleArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new double[]{};
         case ARRAY_SMALL:
            return readDoubles(mkDoubleArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readDoubles(mkDoubleArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readDoubles(new double[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private double[] mkDoubleArray(int len, int limit) {
      return new double[len == 0 ? limit : len];
   }

   private double[] readDoubles(double[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readDouble();
      return arr;
   }

   private float[] readFloatArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new float[]{};
         case ARRAY_SMALL:
            return readFloats(mkFloatArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readFloats(mkFloatArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readFloats(new float[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private float[] mkFloatArray(int len, int limit) {
      return new float[len == 0 ? limit : len];
   }

   private float[] readFloats(float[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readFloat();
      return arr;
   }

   private int[] readIntArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new int[]{};
         case ARRAY_SMALL:
            return readInts(mkIntArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readInts(mkIntArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readInts(new int[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private int[] mkIntArray(int len, int limit) {
      return new int[len == 0 ? limit : len];
   }

   private int[] readInts(int[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readInt();
      return arr;
   }

   private long[] readLongArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new long[]{};
         case ARRAY_SMALL:
            return readLongs(mkLongArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readLongs(mkLongArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readLongs(new long[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private long[] mkLongArray(int len, int limit) {
      return new long[len == 0 ? limit : len];
   }

   private long[] readLongs(long[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readLong();
      return arr;
   }

   private short[] readShortArray(ObjectInput in) throws IOException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new short[]{};
         case ARRAY_SMALL:
            return readShorts(mkShortArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readShorts(mkShortArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readShorts(new short[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private short[] mkShortArray(int len, int limit) {
      return new short[len == 0 ? limit : len];
   }

   private short[] readShorts(short[] arr, ObjectInput in) throws IOException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readShort();
      return arr;
   }

   private Object[] readObjectArray(ObjectInput in) throws IOException, ClassNotFoundException {
      byte type = in.readByte();
      switch (type) {
         case ARRAY_EMPTY:
            return new Object[]{};
         case ARRAY_SMALL:
            return readObjects(mkObjectArray(in.readUnsignedByte(), SMALL), in);
         case ARRAY_MEDIUM:
            return readObjects(mkObjectArray(in.readUnsignedShort(), MEDIUM), in);
         case ARRAY_LARGE:
            return readObjects(new Object[in.readInt()], in);
         default:
            throw new IOException("Unknown array type: " + Integer.toHexString(type));
      }
   }

   private Object[] mkObjectArray(int len, int limit) {
      return new Object[len == 0 ? limit : len];
   }

   private Object[] readObjects(Object[] arr, ObjectInput in) throws IOException, ClassNotFoundException {
      final int len = arr.length;
      for (int i = 0; i < len; i ++) arr[i] = in.readObject();
      return arr;
   }

   @Override
   public Integer getId() {
      return InternalIds.PRIMITIVE;
   }

   private final class PrimitiveTypes extends AbstractSet<Class<?>> {

      @Override
      public boolean contains(Object o) {
         final int doesNotExist = -1;
         int i = PrimitiveExternalizer.this.subIds.get((Class<?>) o, doesNotExist);
         return i != doesNotExist;
      }

      @Override
      public Iterator<Class<?>> iterator() {
         return null;
      }

      @Override
      public int size() {
         return 0;
      }

   }

}
